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

#include <jni.h>
#include "util.h"
#include "vcl/vectorclass.h"
#include "vec_filter.h"
#include "perf_counters.h"

void MULTI_VERSION_NAME (ghash_hit_1)(const int64_t *hashes, bool *b, const int64_t hashes_count, const int32_t hashes_bits,
                                     const int64_t target, const int32_t target_bits) {
    //assert count >=8
    //assert x,y,b equal length >= count
    const int step = 8;
    const size_t limit = hashes_count - step + 1;
    const int32_t hash_shift = 64 - hashes_bits;
    const int64_t hash_mask = ((1 << hashes_bits) - 1) << hash_shift;

    const int64_t target_shift = 64 - target_bits;
    const int32_t target_mask = ((1 << target_bits) - 1) << target_shift;
    const int64_t norm_target = (target << target_shift) & target_mask;

    const Vec8q norm_target_vec(norm_target); // broadcast normalized target
    const Vec8q hash_mask_vec(hash_mask); // broadcast mask

    Vec8q norm_hash_vec;
    Vec8qb target_hit_mask;

    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(hashes + i + 64, _MM_HINT_T1);

        norm_hash_vec.load(hashes + i);
        norm_hash_vec <<= hash_shift;
        norm_hash_vec &= hash_mask_vec;

        target_hit_mask = norm_target_vec == norm_hash_vec;

        for(int j = 0; j < 8; ++j) {
            b[i+j] = target_hit_mask[j];
        }
    }

    for (; i < hashes_count; ++i) {
        const int64_t norm_hash = (hashes[i] << hash_shift) & hash_mask;
        b[i] = norm_target == norm_hash;
    }
}

void MULTI_VERSION_NAME (ghash_hit_n)(const int64_t *hashes, bool *b, const int64_t hashes_count, const int32_t hashes_bits,
                                      const int64_t *targets, const int64_t targets_count) {
    //assert count >=8
    //assert x,y,b equal length >= count
    const int step = 8;
    const size_t limit = hashes_count - step + 1;
    const int32_t hash_shift = 64 - hashes_bits;
    const int64_t hash_mask = ((1 << hashes_bits) - 1) << hash_shift;

    const Vec8q hash_mask_vec(hash_mask); // broadcast mask

    Vec8q norm_hash_vec;

    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(hashes + i + 64, _MM_HINT_T1);

        norm_hash_vec.load(hashes + i);
        norm_hash_vec <<= hash_shift;
        norm_hash_vec &= hash_mask_vec;

        Vec8qb all_targets_hit_mask(false);

        int64_t t = 0;
        while (t < targets_count) {
            const int64_t cluster_key = targets[t];
            const int64_t cluster_bits = targets[t + 1];
            const int64_t cluster_size = targets[t + 2];
            const int64_t cluster_shift = 64 - cluster_bits;
            const int64_t cluster_mask = ((1 << cluster_bits) - 1) << cluster_shift;

            Vec8q cluster_vec((cluster_key << cluster_shift) & cluster_mask);

            Vec8qb target_hit_mask = cluster_vec == norm_hash_vec;
            if (to_bits(target_hit_mask)) {
                if (cluster_size) {
                    for (int64_t h = 0; h < cluster_size; ++h) {
                        const int64_t target_hash = targets[t + h + 3];
                        const int64_t target_hash_bits = targets[t + h + 3 + 1];
                        const int64_t target_hash_shift = 64 - target_hash_bits;
                        const int64_t target_hash_mask = ((1 << target_hash_bits) - 1) << target_hash_shift;

                        Vec8q target_hash_vec((target_hash << target_hash_shift) & target_hash_mask);
                        all_targets_hit_mask |= target_hash_vec == norm_hash_vec;
                    }
                } else {
                    all_targets_hit_mask |= target_hit_mask; // single key
                }
            }
            t += 2 * cluster_size + 3;
        }
        for(int j = 0; j < 8; ++j) {
            b[i + j] = all_targets_hit_mask[j];
        }
    }

    for (; i < hashes_count; ++i) {
        int64_t t = 0;
        bool all_targets_hit = false;
        while (t < targets_count) {
            const int64_t cluster_key = targets[t];
            const int64_t cluster_bits = targets[t + 1];
            const int64_t cluster_size = targets[t + 2];
            const int64_t cluster_shift = 64 - cluster_bits;
            const int64_t cluster_mask = ((1 << cluster_bits) - 1) << cluster_shift;

            const int64_t norm_cluster = (cluster_key << cluster_shift) & cluster_mask;
            const int64_t norm_hash = (hashes[i] << hash_shift) & hash_mask;

            bool cluster_hit = norm_cluster == norm_hash;
            if (cluster_hit) {
                if (cluster_size) {
                    bool targets_hit = false;
                    for (int64_t h = 0; h < cluster_size; ++h) {
                        const int64_t target_hash = targets[t + h + 3];
                        const int64_t target_hash_bits = targets[t + h + 3 + 1];
                        const int64_t target_hash_shift = 64 - target_hash_bits;
                        const int64_t target_hash_mask = ((1 << target_hash_bits) - 1) << target_hash_shift;

                        const int64_t norm_target_hash = (target_hash << target_hash_shift) & target_hash_mask;
                        all_targets_hit |= norm_target_hash == norm_hash;
                    }
                } else {
                    all_targets_hit |= cluster_hit;
                }
            }
            t += 2 * cluster_size + 3;
        }
        b[i] = all_targets_hit;
    }
}

void MULTI_VERSION_NAME (ghash_hit_n2)(const int64_t *hashes, bool *b, const int64_t hashes_count, const int32_t hashes_bits,
                                      const int64_t *targets, const int64_t targets_count) {
    //assert count >=8
    //assert x,y,b equal length >= count
    const int step = 8;
    const size_t limit = hashes_count - step + 1;
    const int32_t hash_shift = 64 - hashes_bits;
    const int64_t hash_mask = ((1 << hashes_bits) - 1) << hash_shift;

    const Vec8q hash_mask_vec(hash_mask); // broadcast mask

    Vec8q norm_hash_vec;
    Vec8qb target_hit_mask;

    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(hashes + i + 64, _MM_HINT_T1);

        norm_hash_vec.load(hashes + i);
        norm_hash_vec <<= hash_shift;
        norm_hash_vec &= hash_mask_vec;

        int64_t t = 0;
        while (t < targets_count) {
            const int64_t cluster_key = targets[t];
            const int64_t cluster_bits = targets[t + 1];
            const int64_t cluster_shift = 64 - cluster_bits;
            const int64_t cluster_mask = ((1 << cluster_bits) - 1) << cluster_shift;

            Vec8q cluster_vec((cluster_key << cluster_shift) & cluster_mask);

            target_hit_mask |= cluster_vec == norm_hash_vec;
            for(int j = 0; j < 8; ++j) {
                b[i + j] = target_hit_mask[j];
            }
            t += 2;
        }
    }

    for (; i < hashes_count; ++i) {
        int64_t t = 0;
        const int64_t norm_hash = (hashes[i] << hash_shift) & hash_mask;
        bool all_targets_hit = false;
        while (t < targets_count) {
            const int64_t cluster_key = targets[t];
            const int64_t cluster_bits = targets[t + 1];
            const int64_t cluster_shift = 64 - cluster_bits;
            const int64_t cluster_mask = ((1 << cluster_bits) - 1) << cluster_shift;
            const int64_t norm_cluster = (cluster_key << cluster_shift) & cluster_mask;

            all_targets_hit |= norm_cluster == norm_hash;
            t += 2;
        }
        b[i] = all_targets_hit;
    }
}

void MULTI_VERSION_NAME (range_select_1)(const double *x, const double *y, bool *b, int64_t count, const drange_t &r) {
    //assert count >=8
    //assert x,y,b equal length >= count
    const int step = 8;
    const size_t limit = count - step + 1;

    const Vec8d xminv(r.xmin);
    const Vec8d xmaxv(r.xmax);
    const Vec8d yminv(r.ymin);
    const Vec8d ymaxv(r.ymax);

    Vec8d xv;
    Vec8d yv;
    Vec8db bv;
    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(x + i + 64, _MM_HINT_T1);
        _mm_prefetch(y + i + 64, _MM_HINT_T1);
        xv.load(x + i);
        yv.load(y + i);

        bv = xminv <= xv && xv <= xmaxv && yminv <= yv && yv <= ymaxv;
        for (int j = 0; j < 8; ++j) {
            b[i + j] = bv[j];
//            b[0] = bv[j];
        }
    }

    for (; i < count; ++i) {
        b[i] = r.xmin <= x[i] && x[i] <= r.xmax && r.ymin <= y[i] && y[i] <= r.ymax;
    }
}

void MULTI_VERSION_NAME (range_select_2)(const double *x, const double *y, bool *b, int64_t count, const drange_t &r) {
    //assert count >=4
    //assert x,y,b equal length >= count
    const int step = 4;
    const size_t limit = count - step + 1;

    const Vec4d xminv4(r.xmin);
    const Vec4d xmaxv4(-r.xmax);
    const Vec4d yminv4(r.ymin);
    const Vec4d ymaxv4(-r.ymax);

    const Vec8d cxv8(xminv4, xmaxv4);
    const Vec8d cyv8(yminv4, ymaxv4);

    Vec4d xv;
    Vec4d yv;
    Vec8db bv;
    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(x + i + 64, _MM_HINT_T1);
        _mm_prefetch(y + i + 64, _MM_HINT_T1);

        xv.load(x + i);
        yv.load(y + i);

        Vec8d xv8(xv, -xv);
        Vec8d yv8(yv, -yv);

        bv = xv8 <= cxv8 && yv8 <= cyv8;
//        if(to_bits(bv) != 0) {
        for (int j = 0; j < 8; ++j) {
            b[i + j] = bv[j];
        }
//        }
    }

    for (; i < count; ++i) {
        b[i] = r.xmin <= x[i] && x[i] <= r.xmax && r.ymin <= y[i] && y[i] <= r.ymax;
    }
}

template<typename T8, typename TB>
void range_select_00(const double *x, bool *b, int64_t count, const double min, const double max) {
    //assert count >=8
    //assert x,y,b equal length >= count
    const int step = 8;
    const size_t limit = count - step + 1;

    const T8 minv(min);
    const T8 maxv(max);

    T8 xv;
    TB bv;
    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(x + i + 64, _MM_HINT_T1);

        xv.load(x + i);

        bv = minv <= xv && xv <= maxv;
        for (int j = 0; j < 8; ++j) {
            b[i + j] = bv[j];
        }
    }

    for (; i < count; ++i) {
        b[i] = min <= x[i] && x[i] <= max;
    }
}

template<typename T4, typename T8, typename TB>
void range_select_01(const double *x, bool *b, int64_t count, const double min, const double max) {
    //assert count >= 4
    //assert x,y,b equal length >= count
    const int step = 4;
    const size_t limit = count - step + 1;

    const T4 minv4(min);
    const T4 maxv4(-max);
    const T8 cxv8(minv4, maxv4);

    T4 xv;
    T4 yv;
    TB bv;
    size_t i = 0;
    for (; i < limit; i += step) {
        _mm_prefetch(x + i + 64, _MM_HINT_T1);
        xv.load(x + i);

        T8 xv8(xv, -xv);
        bv = xv8 <= cxv8;
        for (int j = 0; j < 8; ++j) {
            b[i + j] = bv[j];
        }
    }

    for (; i < count; ++i) {
        b[i] = min <= x[i] && x[i] <= max;
    }
}

void MULTI_VERSION_NAME (range_select_3)(const double *x, const double *y, bool *b, int64_t count, const drange_t &r) {
    range_select_00<Vec8d, Vec8db>(x, b, count, r.xmin, r.xmax);
    range_select_00<Vec8d, Vec8db>(y, b, count, r.ymin, r.ymax);
}

void MULTI_VERSION_NAME (range_select_4)(const double *x, const double *y, bool *b, int64_t count, const drange_t &r) {
    range_select_01<Vec4d, Vec8d, Vec8db>(x, b, count, r.xmin, r.xmax);
    range_select_01<Vec4d, Vec8d, Vec8db>(y, b, count, r.ymin, r.ymax);
}