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
#include "vec/jsonb/serialize.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "gen_cpp/descriptors.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/row_codec/row_codec_base.h"
#include "vec/row_codec/row_codec_v2.h"

namespace doris::vectorized {

TEST(RowCodecTest, vector_encode) {
    std::vector<int32_t> vec = {1, 2, 3, 4, 5};
    std::string vector_str;
    RowCodecV2::encode_col_offet(&vec, &vector_str);
    std::vector<int32_t> vec_decode;
    size_t size;
    RowCodecV2::decode_cids(&vec_decode, vector_str.c_str(), size);

    EXPECT_EQ(vec.size(), vec_decode.size());
    for (int i = 0; i < vec.size(); ++i) {
        EXPECT_EQ(vec[i], vec_decode[i]);
    }
}

TEST(RowCodecTest, bitmap_encode) {
    BitmapValue bv;
    for (int i = 0; i < 10; ++i) {
        bv.add(i);
    }
    std::string bv_str;
    RowCodecV2::encode_bitmap(&bv, &bv_str);
    BitmapValue bv_decode;
    size_t size;
    RowCodecV2::decode_bitmap(&bv_decode, bv_str.c_str(), size);

    EXPECT_EQ(*bv._bitmap, *bv_decode._bitmap);
    BitmapValue bv_empty;
    std::string bv_empty_str;
    RowCodecV2::encode_bitmap(&bv_empty, &bv_empty_str);

    BitmapValue bv_empty_decode;
    RowCodecV2::decode_bitmap(&bv_empty_decode, bv_empty_str.c_str(), size);
    std::cout << "bv_empty._type is: " << bv_empty._type << ", bv_empty_decode._type is: " << bv_empty_decode._type << "\n";
    EXPECT_EQ(bv_empty._type, bv_empty_decode._type);
    // EXPECT_EQ(*bv_empty._bitmap, *bv_empty_decode._bitmap);
}

TEST(RowCodecTest, INT_STRING) {
    vectorized::Block block;
    TabletSchema schema;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType>> cols {
            {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT},
            {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING},
            {"k3", FieldType::OLAP_FIELD_TYPE_TINYINT, 3, TYPE_TINYINT},
            {"k4", FieldType::OLAP_FIELD_TYPE_SMALLINT, 7, TYPE_SMALLINT},
            {"k5", FieldType::OLAP_FIELD_TYPE_BIGINT, 8, TYPE_BIGINT}};
    for (auto t : cols) {
        TabletColumn c;
        c.set_name(std::get<0>(t));
        c.set_type(std::get<1>(t));
        c.set_unique_id(std::get<2>(t));
        schema.append_column(c);
    }
    // 1 int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        block.insert(type_and_name);
    }
    // 2 string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                        "test_string");
        block.insert(type_and_name);
    }
    // 3 tinyint
    {
        auto vec = vectorized::ColumnVector<Int8>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt8>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_tinyint");
        block.insert(type_and_name);
    }
    // 4 smallint
    {
        auto vec = vectorized::ColumnVector<Int16>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt16>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_smallint");
        block.insert(type_and_name);
    }
    // 5 bigint
    {
        auto vec = vectorized::ColumnVector<Int64>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt64>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_bigint");
        block.insert(type_and_name);
    }
    
    MutableColumnPtr col = ColumnString::create();
    vectorized::RowCodec* row_codec = new vectorized::RowCodecV2();
    // serialize
    row_codec->row_encode(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()));
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        TSlotDescriptor tslot;
        tslot.__set_colName(std::get<0>(t));
        if (std::get<3>(t) == TYPE_DECIMAL128I) {
            TypeDescriptor type_desc(std::get<3>(t));
            type_desc.precision = 27;
            type_desc.scale = 9;
            tslot.__set_slotType(type_desc.to_thrift());
        } else {
            TypeDescriptor type_desc(std::get<3>(t));
            tslot.__set_slotType(type_desc.to_thrift());
        }
        tslot.__set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(tslot);
        read_desc.add_slot(slot);
    }
    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
    }
    row_codec->rows_decode(create_data_type_serdes(block.get_data_types()),
                                       static_cast<const ColumnString&>(*col.get()), col_uid_to_idx,
                                       new_block, default_values);
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}
} // namespace doris::vectorized
