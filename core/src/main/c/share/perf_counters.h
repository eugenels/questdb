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


#ifndef QUESTDB_PERF_COUNTERS_H
#define QUESTDB_PERF_COUNTERS_H

#ifdef OOO_CPP_PROFILE_TIMING
#include <atomic>
#include <time.h>

const int perf_counter_length = 42;
extern  std::atomic_ulong perf_counters[perf_counter_length];

inline uint64_t currentTimeNanos() {
    struct timespec timespec;
    clock_gettime(CLOCK_REALTIME, &timespec);
    return timespec.tv_sec * 1000000000L + timespec.tv_nsec;
}
#endif

template<typename T>
inline void measure_time(int index, T func) {
#ifdef OOO_CPP_PROFILE_TIMING
    auto start = currentTimeNanos();
    func();
    auto end = currentTimeNanos();
    perf_counters[index].fetch_add(end - start);
#else
    func();
#endif
}

#endif //QUESTDB_PERF_COUNTERS_H
