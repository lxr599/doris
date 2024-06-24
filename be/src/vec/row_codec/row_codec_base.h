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

#include <cstddef>
#include <unordered_map>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"

namespace doris {
class TabletSchema;
class TupleDescriptor;
}

namespace doris::vectorized {
enum RowCodecVersion : char {
    V_1 = 0x00,
    V_2 = 0x01,
};

class RowCodec {
public:
    RowCodec() = default;
    
    virtual ~RowCodec() = default;

    virtual void row_encode(const TabletSchema& schema, const Block& block, ColumnString& dst,
                               int num_cols, const DataTypeSerDeSPtrs& serdes) = 0;
    // batch rows
    virtual void rows_decode(const DataTypeSerDeSPtrs& serdes, const ColumnString& row_store_col,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values) = 0;
    // single row
    virtual void row_decode(const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values) = 0;
};
}  // namespace doris::vectorized