/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#ifndef QUESTDB_VEC_FILTER_H
#define QUESTDB_VEC_FILTER_H

#include "dispatcher.h"


typedef struct drange {
    double  xmin;
    double  xmax;
    double  ymin;
    double  ymax;
} drange_t;


DECLARE_DISPATCHER_TYPE(ghash_hit_1,    const int64_t *hashes, bool *b, const int64_t count, const int32_t bits, const int64_t target, const int32_t target_bits);
DECLARE_DISPATCHER_TYPE(ghash_hit_n,    const int64_t *hashes, bool *b, const int64_t hashes_count, int32_t hashes_bits, const int64_t *targets, const int64_t targets_count);
DECLARE_DISPATCHER_TYPE(ghash_hit_n2,    const int64_t *hashes, bool *b, const int64_t hashes_count, int32_t hashes_bits, const int64_t *targets, const int64_t targets_count);
DECLARE_DISPATCHER_TYPE(range_select_1, const double *x, const double *y, bool *b, int64_t count, const drange_t& r);
DECLARE_DISPATCHER_TYPE(range_select_2, const double *x, const double *y, bool *b, int64_t count, const drange_t& r);
DECLARE_DISPATCHER_TYPE(range_select_3, const double *x, const double *y, bool *b, int64_t count, const drange_t& r);
DECLARE_DISPATCHER_TYPE(range_select_4, const double *x, const double *y, bool *b, int64_t count, const drange_t& r);
#endif //QUESTDB_VEC_FILTER_H
