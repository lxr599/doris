// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/row_codec/row_codec_v2.h"

#include <assert.h>

#include "util/bitmap_value.h"
#include "olap/olap_common.h"
#include "row_codec_v2.h"
#include "row_codec_utils.h"

namespace doris::vectorized {
    size_t RowCodecV2::fixed_data_size(const Block& block, int num_cols) {
        assert(num_cols <= block.columns());
        uint32_t size = 0;
        for (int i = 0; i < num_cols; ++i) {
            const auto& column = block.get_by_position(i).column;
            if (column->values_have_fixed_size()) {
                size += column->size_of_value_if_fixed();
            }
        }
        return size;
    }

    void RowCodecV2::row_encode(const TabletSchema& schema, const Block& block, ColumnString& dst,
                               int num_cols, const DataTypeSerDeSPtrs& serdes) {
        auto num_rows = block.rows();
        // auto fixed_size = fixed_data_size(block, num_cols);
        assert(num_cols <= block.columns());
        std::string row_string;
        std::string fixed_data_buff;   // data buffer
        std::string var_data_buff;
        // uint8_t flag = 0;
        BitmapValue null_bitmap;

        // [0, 3, 4, 6]
        std::vector<int16_t> col_offset;
        
        for (int i = 0; i < num_rows; ++i) {
            row_string.clear();
            fixed_data_buff.clear();
            var_data_buff.clear();
            null_bitmap.clear();
            col_offset.clear();

            fixed_data_buff.reserve(fixed_data_size(block, num_cols));
            // auto fixed_dst = fixed_data_buff.data();

            for (int j = 0; j < num_cols; ++j) {
                const auto& column = block.get_by_position(j).column;
                const auto& tablet_column = *schema.columns()[j];
                // LOG(INFO) << "col name: " << tablet_column.name() << ", col type: " << static_cast<int>(tablet_column.type());
                if (tablet_column.is_row_store_column()) {
                    // ignore dst row store column
                    continue;
                }
                const auto& col_id = tablet_column.unique_id();
                // todo: flag

                if (column->is_null_at(i)) {
                    LOG(INFO) << "null col id: " << col_id;
                    null_bitmap.add(col_id);
                } else {
                    int size = 0;
                    if (column->is_variable_length()) {
                        serdes[j]->row_codec_v2_serialize(*column, i, &var_data_buff, size);
                        col_offset.emplace_back(size);
                    } else {
                        // std::pair<int, int> num_in_type = 
                        //     schema.get_field_type_interval(block.get_by_position(j).type->get_storage_field_type());
                        serdes[j]->row_codec_v2_serialize(*column, i, &fixed_data_buff, size);
                    }
                }
            }
            // append a row
            row_string.push_back(RowCodecVersion::V_2); // append version
            auto version_size = sizeof(RowCodecVersion::V_2);
            RowCodecV2::encode_bitmap(&null_bitmap, &row_string);    // append bitmap
            auto bitmap_size = row_string.size() - version_size;
            RowCodecV2::encode_col_offet(&col_offset, &row_string);  // append not null ids
            auto col_offset_size = row_string.size() - version_size - bitmap_size;
            row_string.append(fixed_data_buff);   // append data
            row_string.append(var_data_buff);     // append var data
            LOG_EVERY_N(INFO, 10000) << "row store size: " << row_string.size() 
            << ", version_size: " << version_size << ", bitmap_size: " << bitmap_size
            << ", col_offset_size: " << col_offset_size 
            << ", fixed data size: " << fixed_data_buff.size() << ", var data size: " << var_data_buff.size();
            dst.insert_data(row_string.data(), row_string.size());
        }
    }

    void RowCodecV2::rows_decode(const DataTypeSerDeSPtrs& serdes, const ColumnString& row_store_col,
                                 const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                 Block& dst, const std::vector<std::string>& default_values) {
        for (int i = 0; i < row_store_col.size(); ++i) {
            StringRef row_store_ref = row_store_col.get_data_at(i); // row data ref
            row_decode(serdes, row_store_ref.data, row_store_ref.size, col_id_to_idx, dst,
                        default_values);
        }
    }

    void RowCodecV2::row_decode(const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
                                const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                Block& dst, const std::vector<std::string>& default_values) {
        auto pdata = data;
        size_t pdata_size = 0;
        // size_t idx = 0;
        assert(pdata[0] == RowCodecVersion::V_2);
        pdata_size += sizeof(RowCodecVersion::V_2);
        assert(pdata_size <= size);
        pdata += sizeof(RowCodecVersion::V_2);

        BitmapValue null_bitmap;
        RowCodecV2::decode_bitmap(&null_bitmap, &pdata, pdata_size);

        std::vector<int16_t> col_offset;
        RowCodecV2::decode_cids(&col_offset, &pdata, pdata_size);

        // size_t filled_columns = 0;
        for (auto it = col_id_to_idx.begin(); it != col_id_to_idx.end(); ++it) {
            MutableColumnPtr dst_column =
                        dst.get_by_position(it->second).column->assume_mutable();
            // auto field_type = dst.get_by_position(it->second).type;
            auto col_id = it->first;
            auto col_idx = it->second;
            int16_t col_val_size = col_offset[col_idx * 2 + 1] - col_offset[col_idx * 2] + 1;
            if (null_bitmap.contains(col_id)) {
                ColumnNullable* nullable_column = assert_cast<ColumnNullable*>(dst_column.get());
                // assert_cast<ColumnNullable*>(dst_column)->insert_default();
                nullable_column->insert_default();
                // dst_column->insert_default();
            } else if (col_val_size || (!null_bitmap.contains(col_id) && col_val_size == 0)) {
                // LOG(INFO) << "col_offset begin: " << col_offset[col_idx * 2] << ", end: " << col_offset[col_idx * 2 + 1];
                serdes[col_idx]->row_codec_v2_deserialize(*dst_column, pdata + col_offset[col_idx * 2], col_val_size);
                // pdata += col_val_size;
            } else {
                LOG(ERROR) << "invalid col id!";
                return;
            }
        }
    }

    void RowCodecV2::encode_bitmap(BitmapValue* bitmap, std::string* dst) {
        int32_t size = bitmap->getSizeInBytes();
        dst->append(reinterpret_cast<const char*>(&size), sizeof(size)); // append bitmap length

        std::vector<char> bitmap_str(size);
        bitmap->write_to(bitmap_str.data());
        dst->append(bitmap_str.begin(), bitmap_str.end());
    }

    void RowCodecV2::encode_col_offet(std::vector<int16_t>* col_ids, std::string* dst) {
        const int16_t& cids_size = col_ids->size();
        dst->append(reinterpret_cast<const char*>(&cids_size), sizeof(cids_size));
        for (auto& col_id : *col_ids) {
            dst->append(reinterpret_cast<const char*>(&col_id), sizeof(col_id));
        }
    }

    void RowCodecV2::decode_bitmap(BitmapValue* bitmap, const char** dst, size_t& pdata_size) {
        int32_t len = 0;
        memcpy(&len, *dst, sizeof(len)); // get len
        *dst += sizeof(len);
        pdata_size += sizeof(len);
        if (len > 0) {
            std::string bitmap_str(*dst, len);
            bitmap->deserialize(bitmap_str.c_str());
            *dst += len;
            pdata_size += len;
        }
    }

    void RowCodecV2::decode_cids(std::vector<int16_t>* col_ids, const char** dst, size_t& pdata_size) {
        int16_t cid;
        int16_t cid_size = 0;
        memcpy(&cid_size, *dst, sizeof(cid_size));
        *dst += sizeof(cid_size);
        pdata_size += cid_size;
        for (size_t i = 0; i < cid_size; ++i) {
            memcpy(&cid, *dst, sizeof(cid));
            *dst += sizeof(cid);
            pdata_size += sizeof(cid);
            col_ids->emplace_back(cid);
        }
    }

} //namespace doris::vectorized