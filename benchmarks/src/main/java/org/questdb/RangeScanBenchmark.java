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

package org.questdb;

        import io.questdb.cairo.vm.ContiguousVirtualMemory;
        import io.questdb.cairo.vm.PagedVirtualMemory;
        import io.questdb.std.Os;
        import io.questdb.std.Rnd;
        import io.questdb.std.Unsafe;
        import io.questdb.std.Vect;
        import org.openjdk.jmh.annotations.*;
        import org.openjdk.jmh.runner.Runner;
        import org.openjdk.jmh.runner.RunnerException;
        import org.openjdk.jmh.runner.options.Options;
        import org.openjdk.jmh.runner.options.OptionsBuilder;

        import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RangeScanBenchmark {
    private static final Rnd rnd = new Rnd();
    private static final long ROWS_NUMBER = 1000000;
    private final long xaddr;
    private final long yaddr;
    private final long baddr;

    private final double xmin;
    private final double xmax;
    private final double ymin;
    private final double ymax;

    public RangeScanBenchmark() {
        this.xaddr = Unsafe.malloc(8*ROWS_NUMBER);
        this.yaddr = Unsafe.malloc(8*ROWS_NUMBER);
        this.baddr = Unsafe.malloc(1*ROWS_NUMBER);
        clear_bools(this.baddr, ROWS_NUMBER);
        rnd_doubles(this.xaddr, ROWS_NUMBER);
        rnd_doubles(this.yaddr, ROWS_NUMBER);
        double x1 = rnd.nextDouble();
        double x2 = rnd.nextDouble();
        this.xmin = Double.min(x1, x2);
        this.xmax = Double.max(x1, x2);
        double y1 = rnd.nextDouble();
        double y2 = rnd.nextDouble();
        this.ymin = Double.min(y1, y2);
        this.ymax = Double.max(y1, y2);
    }

    public void rnd_doubles(final long address, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(address + i * 8L, rnd.nextDouble());
        }
    }

    public void clear_bools(final long address, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) 0);
        }
    }

    @TearDown(Level.Invocation)
    public void close() {
        Unsafe.free(this.xaddr, 8*ROWS_NUMBER);
        Unsafe.free(this.yaddr, 8*ROWS_NUMBER);
        Unsafe.free(this.baddr, 1*ROWS_NUMBER);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RangeScanBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        clear_bools(this.baddr, ROWS_NUMBER);
    }

    @Benchmark
    public void testRangeSelect1() {
        Vect.RangeSelect1(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
    }
    @Benchmark
    public void testRangeSelect2() {
        Vect.RangeSelect2(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
    }
    @Benchmark
    public void testRangeSelect3() {
        Vect.RangeSelect3(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
    }
    @Benchmark
    public void testRangeSelect4() {
        Vect.RangeSelect4(xaddr, yaddr, baddr, ROWS_NUMBER, xmin, xmax, ymin, ymax);
    }

    static {
        Os.init();
    }
}
