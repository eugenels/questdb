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


#include "util.h"
#include "vec_filter.h"
#include "perf_counters.h"
#include <iostream>
#include <string>
#include <vector>
#include <map>

static const int base32_idxs[]={
        0,  1,  2,  3,  4,  5,  6,  7,  // 30-37, '0'..'7'
        8,  9, -1, -1, -1, -1, -1, -1,  // 38-2F, '8','9'
        -1, -1, 10, 11, 12, 13, 14, 15, // 40-47, 'B'..'G'
        16, -1, 17, 18, -1, 19, 20, -1, // 48-4F, 'H','J','K','M','N'
        21, 22, 23, 24, 25, 26, 27, 28, // 50-57, 'P'..'W'
        29, 30, 31, -1, -1, -1, -1, -1, // 58-5F, 'X'..'Z'
        -1, -1, 10, 11, 12, 13, 14, 15, // 60-67, 'b'..'g'
        16, -1, 17, 18, -1, 19, 20, -1, // 68-6F, 'h','j','k','m','n'
        21, 22, 23, 24, 25, 26, 27, 28, // 70-77, 'p'..'w'
        29, 30, 31,                     // 78-7A, 'x'..'z'
};

bool from_geohash(const std::string &hash, int64_t &output) {
    output = 0;
    for(auto c : hash) {
        int idx = base32_idxs[c - 48];
        if (idx < 0) return false;
        for (int bits = 4; bits >= 0; --bits) {
            output <<= 1;
            output |= ((idx >> bits) & 1) != 0 ? 1 : 0;
        }
    }
    return true;
}

std::string lcp(const std::string &x, const std::string &y) {
    size_t i = 0, j = 0;
    while (i < x.length() && j < y.length()) {
        if (x[i] != y[j]) {
            break;
        }
        i++, j++;
    }
    return x.substr(0, i);
}

bool is_prefix(const std::string& p, const std::string& s) {
    return  !p.empty()
            && p.size() < s.size()
            && std::mismatch(p.begin(), p.end(), s.begin(), s.end()).first == p.end();
}

std::map<std::string, std::vector<std::string>> prefix_partition(std::vector<std::string> &hashes) {
    std::map<std::string, std::vector<std::string>> groups;
    std::sort(hashes.begin(), hashes.end());
    size_t i = 0;
    while(i < hashes.size() - 1) {
        std::string key = lcp(hashes[i], hashes[i + 1]);
        if (key != hashes[i]) {
            groups[key].push_back(hashes[i]);
        } else {
            groups[hashes[i]];
        }
        i++;
        while(is_prefix(key, hashes[i]) && i < hashes.size()) {
            groups[key].push_back(hashes[i++]);
        }
    }
    return groups;
}

std::vector<int64_t> group_by_prefix(std::vector<std::string> &hashes_str) {
    std::vector<int64_t> bhashes;
    std::map<std::string, std::vector<std::string>> bins = prefix_partition(hashes_str);
    for(auto const& g : bins) {
        int64_t key = 0;
        if(from_geohash(g.first, key)) {
            std::cout << g.first << " " << key << " " << 5*g.first.size() << std::endl;
            std::cout << "--------" << std::endl;
            bhashes.push_back(key);
            bhashes.push_back(5*g.first.size()); //bits
            bhashes.push_back(g.second.size());
            for (auto const &h : g.second) {
                int64_t hkey = 0;
                if(from_geohash(h, hkey)) {
                    bhashes.push_back(hkey);
                    bhashes.push_back(5*h.size()); //bits
                    std::cout << "\t" << h << " " << hkey << " " << 5*h.size() << std::endl;
                }
            }
        }
    }
    return bhashes;
}

std::vector<int64_t> encode_hashes(const std::vector<std::string> &hashes_str) {
    std::vector<int64_t> bhashes;
    for(auto const& h : hashes_str) {
        int64_t key = 0;
        if(from_geohash(h, key)) {
            bhashes.push_back(key);
            bhashes.push_back(5*h.size());
            std::cout << h << " " << key << " " << 5*h.size() << std::endl;
        }
    }
    std::cout << "--------" << std::endl;
    return bhashes;
}

extern "C" {
    DECLARE_DISPATCHER(ghash_hit_1);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_GeoHashHit1
    (JNIEnv *e, jclass cl, jlong hddr, jlong baddr, jlong size,
            jint bits, jlong thash, jint tbits) {
        const int64_t *hashes = reinterpret_cast<int64_t *>(hddr);
        bool *bitmask = reinterpret_cast<bool *>(baddr);
        const int64_t hashes_count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        const int32_t hashes_bits = static_cast<int32_t>(bits);
        const int64_t target = __JLONG_REINTERPRET_CAST__(int64_t, thash);
        const int32_t target_bits = static_cast<int32_t>(tbits);
        measure_time(32, [=]() {
            ghash_hit_1(hashes, bitmask, hashes_count, hashes_bits, target, target_bits);
        });
    }

    DECLARE_DISPATCHER(ghash_hit_n);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_GeoHashHitN
    (JNIEnv *e, jclass cl, jlong hddr, jlong baddr, jlong size,
            jint bits, jlong taddr, jlong tcount) {
        const int64_t *hashes = reinterpret_cast<int64_t *>(hddr);
        bool *bitmask = reinterpret_cast<bool *>(baddr);
        const int64_t hashes_count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        const int32_t hashes_bits = static_cast<int32_t>(bits);
        const int64_t *targets = reinterpret_cast<const int64_t *>(taddr);
        const int64_t targets_count = __JLONG_REINTERPRET_CAST__(int64_t, tcount);
        measure_time(37, [=]() {
            ghash_hit_n(hashes, bitmask, hashes_count, hashes_bits, targets, targets_count);
        });
    }

    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_GeoHashHitNStr
    (JNIEnv *e, jclass cl, jlong hddr, jlong baddr, jlong size,
            jint bits, jobjectArray targetsStr) {
        const int64_t *hashes = reinterpret_cast<int64_t *>(hddr);
        bool *bitmask = reinterpret_cast<bool *>(baddr);
        const int64_t hashes_count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        const int32_t hashes_bits = static_cast<int32_t>(bits);

        int stringCount = e->GetArrayLength(targetsStr);
        std::vector<std::string> hashes_str;
        for (int i=0; i<stringCount; i++) {
            jstring string = (jstring) (e->GetObjectArrayElement(targetsStr, i));
            const char *rawString = e->GetStringUTFChars(string, 0);
            hashes_str.push_back(rawString);
            e->ReleaseStringUTFChars(string, rawString);
        }
        std::vector<int64_t> hashes_int = group_by_prefix(hashes_str);
        const int64_t *targets = reinterpret_cast<const int64_t *>(hashes_int.data());
        const int64_t targets_count = __JLONG_REINTERPRET_CAST__(int64_t, hashes_int.size());
        measure_time(37, [=]() {
            ghash_hit_n(hashes, bitmask, hashes_count, hashes_bits, targets, targets_count);
        });
    }

    DECLARE_DISPATCHER(ghash_hit_n2);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_GeoHashHitNStr2
    (JNIEnv *e, jclass cl, jlong hddr, jlong baddr, jlong size,
            jint bits, jobjectArray targetsStr) {
        const int64_t *hashes = reinterpret_cast<int64_t *>(hddr);
        bool *bitmask = reinterpret_cast<bool *>(baddr);
        const int64_t hashes_count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        const int32_t hashes_bits = static_cast<int32_t>(bits);

        int stringCount = e->GetArrayLength(targetsStr);
        std::vector<std::string> hashes_str;
        for (int i=0; i<stringCount; i++) {
            jstring string = (jstring) (e->GetObjectArrayElement(targetsStr, i));
            const char *rawString = e->GetStringUTFChars(string, 0);
            hashes_str.push_back(rawString);
            e->ReleaseStringUTFChars(string, rawString);
        }
        std::vector<int64_t> hashes_int = encode_hashes(hashes_str);
        const int64_t *targets = reinterpret_cast<const int64_t *>(hashes_int.data());
        const int64_t targets_count = __JLONG_REINTERPRET_CAST__(int64_t, hashes_int.size());
        measure_time(38, [=]() {
            ghash_hit_n2(hashes, bitmask, hashes_count, hashes_bits, targets, targets_count);
        });
    }

    DECLARE_DISPATCHER(range_select_1);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_RangeSelect1
    (JNIEnv *e, jclass cl, jlong xaddr, jlong yaddr, jlong baddr, jlong size,
            jdouble xmin, jdouble xmax, jdouble ymin, jdouble ymax) {
        drange_t range = {xmin, xmax, ymin, ymax};
        const double *x = reinterpret_cast<double *>(xaddr);
        const double *y = reinterpret_cast<double *>(yaddr);
        bool *b = reinterpret_cast<bool *>(baddr);
        const int64_t count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        measure_time(33, [=]() {
            range_select_1(x, y, b, count, range);
        });
    }

    DECLARE_DISPATCHER(range_select_2);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_RangeSelect2
    (JNIEnv *e, jclass cl, jlong xaddr, jlong yaddr, jlong baddr, jlong size,
            jdouble xmin, jdouble xmax, jdouble ymin, jdouble ymax) {
        drange_t range = {xmin, xmax, ymin, ymax};
        const double *x = reinterpret_cast<double *>(xaddr);
        const double *y = reinterpret_cast<double *>(yaddr);
        bool *b = reinterpret_cast<bool *>(baddr);
        const int64_t count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        measure_time(34, [=]() {
            range_select_2(x, y, b, count, range);
        });
    }

    DECLARE_DISPATCHER(range_select_3);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_RangeSelect3
    (JNIEnv *e, jclass cl, jlong xaddr, jlong yaddr, jlong baddr, jlong size,
            jdouble xmin, jdouble xmax, jdouble ymin, jdouble ymax) {
        drange_t range = {xmin, xmax, ymin, ymax};
        const double *x = reinterpret_cast<double *>(xaddr);
        const double *y = reinterpret_cast<double *>(yaddr);
        bool *b = reinterpret_cast<bool *>(baddr);
        const int64_t count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        measure_time(35, [=]() {
            range_select_3(x, y, b, count, range);
        });
    }

    DECLARE_DISPATCHER(range_select_4);
    JNIEXPORT void JNICALL
    Java_io_questdb_std_Vect_RangeSelect4
    (JNIEnv *e, jclass cl, jlong xaddr, jlong yaddr, jlong baddr, jlong size,
            jdouble xmin, jdouble xmax, jdouble ymin, jdouble ymax) {
        drange_t range = {xmin, xmax, ymin, ymax};
        const double *x = reinterpret_cast<double *>(xaddr);
        const double *y = reinterpret_cast<double *>(yaddr);
        bool *b = reinterpret_cast<bool *>(baddr);
        const int64_t count = __JLONG_REINTERPRET_CAST__(int64_t, size);
        measure_time(36, [=]() {
            range_select_4(x, y, b, count, range);
        });
    }
}
