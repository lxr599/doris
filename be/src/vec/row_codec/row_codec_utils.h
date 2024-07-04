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

#include "gutil/endian.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

template<class T>
inline T make_big_endian(T value) {
    LOG(ERROR) << "make_big_endian() not implemented for type";
    return sizeof(value);
}

template<>
inline int8_t make_big_endian(int8_t value) {
    return value;
}

template<>
inline int16_t make_big_endian(int16_t value) {
    return BigEndian::FromHost16(value);
}

template<>
inline int32_t make_big_endian(int32_t value) {
    return BigEndian::FromHost32(value);
}

template<>
inline int64_t make_big_endian(int64_t value) {
    return BigEndian::FromHost64(value);
}

template<typename D>
void append_to_buff(std::string* dst, const StringRef& data_ref) {
    D val = *reinterpret_cast<const D*>(data_ref.data);
    dst->append(reinterpret_cast<const char*>(&val), sizeof(val));
}

} // namespace doris::vectorized