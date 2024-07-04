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

#pragma once

#include "vec/row_codec/row_codec_base.h"

namespace doris::vectorized {
// class row {
// public:

// private:
//     uint8_t _version;
//     uint8_t _flag;
//     BitmapValue _null_bitmap;
//     std::vector<int8_t> _col_id_8;
//     std::vector<int16_t> _col_id_16;
// };

class RowCodecV2 : public RowCodec{
public:
    RowCodecV2() {}

    ~RowCodecV2() = default;

     void row_encode(const TabletSchema& schema, const Block& block, ColumnString& dst,
                               int num_cols, const DataTypeSerDeSPtrs& serdes);
    // batch rows
     void rows_decode(const DataTypeSerDeSPtrs& serdes, const ColumnString& row_store_col,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values);
    // single row
     void row_decode(const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values);
    
    static size_t fixed_data_size(const Block& block, int num_cols);

    static void encode_bitmap(BitmapValue* bitmap, std::string* dst);
    static void encode_col_offet(std::vector<int16_t>* col_ids, std::string* dst);

    static void decode_bitmap(BitmapValue* bitmap, const char** dst, size_t& pdata_size);
    static void decode_cids(std::vector<int16_t>* col_ids, const char** dst, size_t& pdata_size);
};
} // namespace doris::vectorized