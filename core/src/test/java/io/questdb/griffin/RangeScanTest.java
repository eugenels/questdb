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

package io.questdb.griffin;

import io.questdb.cairo.vm.ReadOnlyVirtualMemory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.junit.*;

import java.util.concurrent.ThreadLocalRandom;

public class RangeScanTest {
    private static final Rnd rnd = new Rnd();
//    private static final long ROWS_NUMBER = 1000000000;
    private static final long ROWS_NUMBER = 250000;
    private static final long RUNS_NUMBER = 5;
    private static final int BITS = 8;
    private static long haddr;
    private static long xaddr;
    private static long yaddr;
    private static long baddr;

    private static double xmin;
    private static double xmax;
    private static double ymin;
    private static double ymax;
    private static long target_hash;

    public static void rnd_doubles(final long address, long len, double min, double max) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(address + i * 8L, rnd_double(min, max));
        }
    }

    public static void rnd_longs(final long address, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(address + i * 8L, rnd.nextLong());
        }
    }

    public static double rnd_double(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    public static long rnd_geohash() {
        double x = rnd_double(-90, 90);
        double y = rnd_double(-180, 180);
        return encodeGeohash(x, y, 5*BITS);
    }

    public static void rnd_geohashes(final long address, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(address + i * 8L, rnd_geohash());
        }
    }

    public static void clear_bools(final long address, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) 0);
        }
    }

    public static long binaryHash(String bit_string) {
        long bits = 0;
        for (int i = 0; i < bit_string.length(); i++) {
            char c = bit_string.charAt(i);
            if(c == '1') {
                bits = (bits << 1) | 1;
            } else {
                bits = bits << 1;
            }
        }
        return bits;
    }

    public static long encodeGeohash(double lat, double lng, int bits) {
        double minLat = -90,  maxLat = 90;
        double minLng = -180, maxLng = 180;
        long result = 0;
        for (int i = 0; i < bits; ++i)
            if (i % 2 == 0) {
                double midpoint = (minLng + maxLng) / 2;
                if (lng < midpoint) {
                    result <<= 1;
                    maxLng = midpoint;
                } else {
                    result = result << 1 | 1;
                    minLng = midpoint;
                }
            } else {
                double midpoint = (minLat + maxLat) / 2;
                if (lat < midpoint) {
                    result <<= 1;
                    maxLat = midpoint;
                } else {
                    result = result << 1 | 1;
                    minLat = midpoint;
                }
            }
        return result;
    }

    @BeforeClass
    public static void start() {
        haddr = Unsafe.malloc(8*ROWS_NUMBER);
//        xaddr = Unsafe.malloc(8*ROWS_NUMBER);
//        yaddr = Unsafe.malloc(8*ROWS_NUMBER);
        baddr = Unsafe.malloc(1*ROWS_NUMBER);
        clear_bools(baddr, ROWS_NUMBER);
        rnd_geohashes(haddr, ROWS_NUMBER);
//        rnd_doubles(xaddr, ROWS_NUMBER, -90, 90);
//        rnd_doubles(yaddr, ROWS_NUMBER, -180, 180);
        double x1 = rnd_double(-90, 90);
        double x2 = rnd_double(-90, 90);
        xmin = Double.min(x1, x2);
        xmax = Double.max(x1, x2);
        double y1 = rnd_double(-180, 180);
        double y2 = rnd_double(-180, 180);
        ymin = Double.min(y1, y2);
        ymax = Double.max(y1, y2);
//        target_hash = rnd_geohash();
        target_hash = 25902118L << (64 - 5*5);
        System.out.println("start rows: " + ROWS_NUMBER);
    }

    @AfterClass
    public static void stop() {
        Unsafe.free(haddr, 8*ROWS_NUMBER);
//        Unsafe.free(xaddr, 8*ROWS_NUMBER);
//        Unsafe.free(yaddr, 8*ROWS_NUMBER);
        Unsafe.free(baddr, 1*ROWS_NUMBER);
        System.out.println("stop");
    }

    @Before
    public void reset() {
        Vect.resetPerformanceCounters();
        clear_bools(baddr, ROWS_NUMBER);
    }

    @After
    public void shutdown() {
    }

    @Test
    public void testRndDouble() {
        for (int i = 0; i < 1000; ++i) {
            double d1 = rnd_double(-90, 90);
            double d2 = rnd_double(-180, 180);
            assert(-90 <= d1 && d1 <= 90);
            assert(-180 <= d2 && d2 <= 180);
        }
    }

    @Test
    public void testHash() {
        double lat = 31.23;
        double lon = 121.473;
        assert(encodeGeohash(lat,lon, 1*5)==binaryHash("11100"));
        assert(encodeGeohash(lat,lon, 2*5)==binaryHash("1110011001"));
        assert(encodeGeohash(lat,lon, 3*5)==binaryHash("111001100111100"));
        assert(encodeGeohash(lat,lon, 4*5)==binaryHash("11100110011110000011"));
        assert(encodeGeohash(lat,lon, 5*5)==binaryHash("1110011001111000001111000"));
        assert(encodeGeohash(lat,lon, 6*5)==binaryHash("111001100111100000111100010001"));
        assert(encodeGeohash(lat,lon, 7*5)==binaryHash("11100110011110000011110001000110001"));
        assert(encodeGeohash(lat,lon, 8*5)==binaryHash("1110011001111000001111000100011000111111"));
    }

    @Test
    public void testBFSearch() {
        long rows = 1000000000;
        long haddr = Unsafe.malloc(8*rows);
//        for (int i = 0; i < rows; ++i) {
//            Unsafe.getUnsafe().putLong(haddr+i * 8L, i);
//        }
//        for (int i = 0; i < rows; ++i) {
//            long v = Unsafe.getUnsafe().getLong(haddr+i * 8L);
//            System.err.println(v);
//        }
        rnd_longs(haddr, rows);
        long v = rnd.nextLong();
//        long v = rows - 1;
        long jcounter = 0;
        long ccounter = 0;
        long index = -42;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            long start = System.nanoTime();
            index = Vect.branchFreeSearch64Bit(haddr, rows, v);
            long stop = System.nanoTime();
            ccounter += Vect.getPerformanceCounter(39);
            jcounter += stop - start;
        }
        System.err.println("bf_search jns: "+jcounter / RUNS_NUMBER);
        System.err.println("bf_search cns: "+ccounter / RUNS_NUMBER);

        long val = Unsafe.getUnsafe().getLong(haddr + index * 8L);
        System.err.println("bf_search value: "+ val + ", at index: " + index);
        Unsafe.free(haddr, 8*rows);
    }

    static long searchValueBlock(long memory, long offset, long cellCount, long value) {
        // when block is "small", we just scan it linearly
        if (cellCount < 64) {
            // this will definitely exit because we had checked that at least the last value is greater than value
            for (long i = offset; ; i += 8) {
                if (Unsafe.getUnsafe().getLong(memory + i) > value) {
                    return (i - offset) / 8;
                }
            }
        } else {
            // use binary search on larger block
            long low = 0;
            long high = cellCount - 1;
            long half;
            long pivot;
            do {
                half = (high - low) / 2;
                if (half == 0) {
                    break;
                }
                pivot = Unsafe.getUnsafe().getLong(memory + offset + (low + half) * 8);
                if (pivot <= value) {
                    low += half;
                } else {
                    high = low + half;
                }
            } while (true);

            return low + 1;
        }
    }

    @Test
    public void testSearch() {
        long rows = 1000000000;
        long haddr = Unsafe.malloc(8*rows);
//        for (int i = 0; i < rows; ++i) {
//            Unsafe.getUnsafe().putLong(haddr+i * 8L, i);
//        }
//        for (int i = 0; i < rows; ++i) {
//            long v = Unsafe.getUnsafe().getLong(haddr+i * 8L);
//            System.err.println(v);
//        }
        rnd_longs(haddr, rows);
        long v = rnd.nextLong();
//        long v = rows - 1;
        long jcounter = 0;
        long ccounter = 0;
        long index = -42;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            long start = System.nanoTime();
            index = searchValueBlock(haddr, 0, rows, v);
            long stop = System.nanoTime();
            ccounter += Vect.getPerformanceCounter(39);
            jcounter += stop - start;
        }
        System.err.println("b_search jns: "+jcounter / RUNS_NUMBER);
        System.err.println("b_search cns: "+ccounter / RUNS_NUMBER);

        long val = Unsafe.getUnsafe().getLong(haddr + index * 8L);
        System.err.println("b_search value: "+ val + ", at index: " + index);
        Unsafe.free(haddr, 8*rows);
    }

    @Test
    public void testGeoHashHit1() {
        long counter = 0;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.GeoHashHit1(haddr, baddr, ROWS_NUMBER, 5*BITS, target_hash);
            counter += Vect.getPerformanceCounter(32);
        }
        System.err.println("geohash_hit_1: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testGeoHashHitNStr() {
        long counter = 0;
//        String[] hashes = {"xwc3rdm", "xwc3rf22", "dwzc", "dwzckd60", "v9pvp887", "v9pvp2x5", "w23b", "tzb2", "v9nyyf",
//                "tw21eymu", "u2edm7", "w7v9c", "9egcyxd", "sgf3g", "f9d9g", "f3pznu", "vepe0", "u5h49t", "c42rzzg", "fetd"
//        };

        String[] hashes = {"xwc3rdm", "xwc3rf22", "dwzc", "dwzckd60", "v9pvp887", "v9pvp2x5", "w23b", "tzb2", "v9nyyf",
                "tw21eymu", "u2edm7", "w7v9c", "9egcyxd", "sgf3g", "f9d9g", "f3pznu", "vepe0", "u5h49t", "c42rzzg", "fetd"
               ,  "c23n8pg", "z33fj", "8zwkm7", "f2vxde", "c28u1b", "8seud9", "z0zp", "wtrn", "z31y326", "c23n8p", "y06wepy"
        };
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.GeoHashHitNStr(haddr, baddr, ROWS_NUMBER, 5*BITS, hashes);
            counter += Vect.getPerformanceCounter(37);
        }
        System.err.println("geohash_hit_n: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testGeoHashHitNStr2() {
        long counter = 0;
//        String[] hashes = {"xwc3rdm", "xwc3rf22", "dwzc", "dwzckd60", "v9pvp887", "v9pvp2x5", "w23b", "tzb2", "v9nyyf",
//            "tw21eymu", "u2edm7", "w7v9c", "9egcyxd", "sgf3g", "f9d9g", "f3pznu", "vepe0", "u5h49t", "c42rzzg", "fetd"
//        };
        String[] hashes = {"xwc3rdm", "xwc3rf22", "dwzc", "dwzckd60", "v9pvp887", "v9pvp2x5", "w23b", "tzb2", "v9nyyf",
                "tw21eymu", "u2edm7", "w7v9c", "9egcyxd", "sgf3g", "f9d9g", "f3pznu", "vepe0", "u5h49t", "c42rzzg", "fetd"
                ,  "c23n8pg", "z33fj", "8zwkm7", "f2vxde", "c28u1b", "8seud9", "z0zp", "wtrn", "z31y326", "c23n8p", "y06wepy"
        };
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.GeoHashHitNStr2(haddr, baddr, ROWS_NUMBER, 5*BITS, hashes);
            counter += Vect.getPerformanceCounter(38);
        }
        System.err.println("geohash_hit_n2: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testRangeSelect1() {
        long counter = 0;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.RangeSelect1(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
            counter += Vect.getPerformanceCounter(33);
        }
        System.err.println("select_1: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testRangeSelect2() {
        long counter = 0;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.RangeSelect2(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
            counter += Vect.getPerformanceCounter(34);
        }
        System.err.println("select_2: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testRangeSelect3() {
        long counter = 0;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.RangeSelect3(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
            counter += Vect.getPerformanceCounter(35);
        }
        System.err.println("select_3: "+counter / RUNS_NUMBER);
    }

    @Test
    public void testRangeSelect4() {
        long counter = 0;
        for (int i = 0; i < RUNS_NUMBER; ++i) {
            Vect.resetPerformanceCounters();
            Vect.RangeSelect4(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
            counter += Vect.getPerformanceCounter(36);
        }
        System.err.println("select_4: "+counter / RUNS_NUMBER);
    }

    static {
        Os.init();
    }
}
