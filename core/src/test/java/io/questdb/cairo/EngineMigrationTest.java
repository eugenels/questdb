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

package io.questdb.cairo;

import io.questdb.cairo.vm.PagedMappedReadWriteMemory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.EngineMigration.*;
import static io.questdb.cairo.TableUtils.*;

public class EngineMigrationTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testMigrateTableNoSymbolsNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithMonthPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.MONTH)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 3
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithYearPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.YEAR)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2017-01-01", 3
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        10, "2020-01-01", 0
                );

                String query = "select distinct s1, s2 from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayPartitionsAndSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String query = "select distinct s1, s2 from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayRemovedPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String queryOld = "select sum(c1) from src where ts not in '2020-01-01'";
                String queryNew = "select sum(c1) from src";
                LongList removedTimestamps = new LongList();
                removedTimestamps.add(TimestampFormatUtils.parseTimestamp("2020-01-01T00:00:00.000Z"));
                assertMigration(src, queryOld, queryNew, removedTimestamps);
            }
        });
    }

    @Test
    public void testAssignTableId() throws Exception {
        assertMemoryLeak(() -> {
            // This test has to run in a separate engine from the base test engine
            // because of removal of mapped file _tab_index.d with every test
            try (CairoEngine engine = new CairoEngine(configuration)) {
                // roll table id up
                for (int i = 0; i < 10; i++) {
                    engine.getNextTableId();
                }
                String tableName = "test";
                // old table
                try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
                ) {
                    CairoTestUtils.createTableWithVersion(model, 416);
                    downgradeTxFile(model, null);
                }

                try (TableModel model = new TableModel(configuration, "test2", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
                ) {
                    TableUtils.createTable(
                            model.getCairoCfg().getFilesFacade(),
                            model.getMem(),
                            model.getPath(),
                            model.getCairoCfg().getRoot(),
                            model,
                            model.getCairoCfg().getMkDirMode(),
                            ColumnType.VERSION,
                            (int) engine.getNextTableId()
                    );
                }

                // we need to remove "upgrade" file for the engine to upgrade tables
                // remember, this is the second instance of the engine
                assertRemoveUpgradeFile();

                try (CairoEngine engine2 = new CairoEngine(configuration)) {
                    // check if constructor upgrades test
                    try (TableReader reader = engine2.getReader(sqlExecutionContext.getCairoSecurityContext(), "test")) {
                        Assert.assertEquals(12, reader.getMetadata().getId());
                    }
                    try (TableReader reader = engine2.getReader(sqlExecutionContext.getCairoSecurityContext(), "test2")) {
                        Assert.assertEquals(11, reader.getMetadata().getId());
                    }
                }
            }
        });
    }

    @Test
    public void testCannotReadMetadata() throws Exception {
        assertMemoryLeak(() -> {
            // roll table id up
            for (int i = 0; i < 10; i++) {
                engine.getNextTableId();
            }
            String tableName = "test";
            // old table
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
            }

            FilesFacade ff = new FilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    return 0;
                }
            };

            // we need to remove "upgrade" file for the engine to upgrade tables
            // remember, this is the second instance of the engine

            assertRemoveUpgradeFile();

            try {
                new CairoEngine(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                });
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not update table");
            }
        });
    }

    @Test
    public void testMigrateTxFileFailsToSaveTableMetaVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "test", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
                downgradeTxFile(model, null);
            }
            assertRemoveUpgradeFile();

            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_VERSION, "meta");

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try {
                try (CairoEngine ignored = new CairoEngine(config)) {
                    Assert.fail();
                }
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("failed to write updated version to table Metadata file"));
            }
        });
    }

    @Test
    public void testMigrateFailsToSaveTableMetaId() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "test", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
                downgradeTxFile(model, null);
            }
            assertRemoveUpgradeFile();

            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_TABLE_ID, "meta");

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try {
                try (CairoEngine ignored = new CairoEngine(config)) {
                    Assert.fail();
                }
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("Could not update table id"));
            }
        });
    }

    @Test
    public void testMigrateToSaveGlobalUpdateVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "test", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
                downgradeTxFile(model, null);
            }
            assertRemoveUpgradeFile();

            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_TABLE_ID, TableUtils.UPGRADE_FILE_NAME);

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            CairoEngine ignored = new CairoEngine(config);
            // Migration should be successful, not exceptions
            ignored.close();
        });
    }

    @Test
    public void testMigrateTableSimple() throws Exception {
        configOverrideMaxUncommittedRows = 50001;
        configOverrideO3CommitHysteresisInMicros = 777777;

        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select sum(c1) from src";
                assertMetadataMigration(src, query);
            }
        });
    }

    @Test
    public void testCannotUpdateHysteresisMetadata1() throws Exception {
        configOverrideMaxUncommittedRows = 1231231;
        configOverrideO3CommitHysteresisInMicros = 85754;
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                ff = new FilesFacadeImpl() {
                    @Override
                    public long write(long fd, long buf, long len, long offset) {
                        if (META_OFFSET_O3_MAX_UNCOMMITTED_ROWS == offset) {
                            return 0;
                        }
                        return super.write(fd, buf, len, offset);
                    }
                };

                try {
                    assertMetadataMigration(src, "select sum(c1) from src");
                    Assert.fail();
                } catch (SqlException e) {
                    Chars.contains(e.getFlyweightMessage(), "Metadata version does not match runtime version");
                }

                ff = new FilesFacadeImpl() {
                    @Override
                    public long write(long fd, long buf, long len, long offset) {
                        if (META_OFFSET_O3_COMMIT_HYSTERESIS_IN_MICROS == offset) {
                            return 0;
                        }
                        return super.write(fd, buf, len, offset);
                    }
                };

                try {
                    new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
                    assertMetadataMigration(src, "select sum(c1) from src");
                    Assert.fail();
                } catch (SqlException e) {
                    Chars.contains(e.getFlyweightMessage(), "Metadata version does not match runtime version");
                }

                ff = new FilesFacadeImpl();
                new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
                assertMetadataMigration(src, "select sum(c1) from src");
            }
        });
    }

    private void assertMetadataMigration(TableModel src, String query) throws SqlException {
        assertMetadataMigration(src, query, query);
    }

    private void assertMetadataMigration(TableModel src, String queryOld, String queryNew) throws SqlException {
        CharSequence expected = executeSql(queryOld).toString();
        if (!queryOld.equals(queryNew)) {
            // if queries are different they must produce different results
            CharSequence expectedNewEquivalent = executeSql(queryNew).toString();
            Assert.assertNotEquals(expected, expectedNewEquivalent);
        }

        // Downgrade version meta
        downgradeMetaDataFile(src);

        // Act
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);

        // Verify
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Second run of migration should not do anything
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Third time, downgrade and migrate
        downgradeMetaDataFile(src);
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        assertSql("select o3maxUncommittedRows, o3CommitHysteresisMicros from tables where name = '" + src.getName() + "'",
                "o3maxUncommittedRows\to3CommitHysteresisMicros\n" +
                        +configOverrideMaxUncommittedRows + "\t" + configOverrideO3CommitHysteresisInMicros + "\n");
    }

    private void downgradeMetaDataFile(TableModel tableModel) {
        engine.clear();
        FilesFacade ff = configuration.getFilesFacade();

        try (Path path = new Path()) {
            setMetadataVersion(tableModel, ff, path, VERSION_TBL_META_HYSTERESIS);

            path.concat(root).concat(tableModel.getName()).concat(TableUtils.META_FILE_NAME);
            long fd = ff.openRO(path.$());
            Assert.assertTrue(fd >= 0);

            long fileSize = ff.length(fd);
            ff.close(fd);
            try (PagedMappedReadWriteMemory rwTx = new PagedMappedReadWriteMemory(ff, path.$(), fileSize)) {
                rwTx.putInt(META_OFFSET_O3_MAX_UNCOMMITTED_ROWS, 0);
                rwTx.putLong(META_OFFSET_O3_COMMIT_HYSTERESIS_IN_MICROS, 0);
                rwTx.jumpTo(fileSize);
            }

            setMetadataVersion(tableModel, ff, path, VERSION_TBL_META_HYSTERESIS);
            downgradeUpdateFileTo(ff, path);
        }
    }

    private void setMetadataVersion(TableModel tableModel, FilesFacade ff, Path path, int version) {
        int pathLen = path.length();

        try {
            path.trimTo(0).concat(root).concat(tableModel.getName()).concat(TableUtils.META_FILE_NAME);
            long fd = ff.openRO(path.$());
            Assert.assertTrue(fd >= 0);

            long fileSize = ff.length(fd);
            ff.close(fd);
            try (PagedMappedReadWriteMemory rwTx = new PagedMappedReadWriteMemory(ff, path.$(), fileSize)) {
                if (rwTx.getInt(META_OFFSET_VERSION) > version - 1) {
                    rwTx.putInt(META_OFFSET_VERSION, version - 1);
                    rwTx.jumpTo(fileSize);
                }
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void downgradeUpdateFileTo(FilesFacade ff, Path path) {
        path.trimTo(0).concat(root).concat(UPGRADE_FILE_NAME);
        if (ff.exists(path.$())) {
            try (PagedMappedReadWriteMemory rwTx = new PagedMappedReadWriteMemory(ff, path.$(), 8)) {
                rwTx.putInt(0, EngineMigration.VERSION_TBL_META_HYSTERESIS - 1);
                rwTx.jumpTo(Integer.BYTES);
            }
        }
    }

    private FilesFacadeImpl failToWriteMetaOffset(final long metaOffsetVersion, final String filename) {
        return new FilesFacadeImpl() {
            private long metaFd = -1;

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (name.toString().contains(filename)) {
                    this.metaFd = fd;
                }

                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == metaFd && offset == metaOffsetVersion) {
                    return 0;
                }
                return super.write(fd, address, len, offset);
            }
        };
    }

    private static void assertRemoveUpgradeFile() {
        try (Path path = new Path()) {
            path.of(configuration.getRoot()).concat(TableUtils.UPGRADE_FILE_NAME).$();
            Assert.assertTrue(!FilesFacadeImpl.INSTANCE.exists(path) || FilesFacadeImpl.INSTANCE.remove(path));
        }
    }

    private static DateFormat getPartitionDateFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    private void assertMigration(TableModel src, String query) throws SqlException {
        assertMigration(src, query, query, null);
    }

    private void assertMigration(TableModel src, String queryOld, String queryNew, LongList removedPartitions) throws SqlException {
        CharSequence expected = executeSql(queryOld).toString();
        if (!queryOld.equals(queryNew)) {
            // if queries are different they must produce different results
            CharSequence expectedNewEquivalent = executeSql(queryNew).toString();
            Assert.assertNotEquals(expected, expectedNewEquivalent);
        }

        // There are no symbols, no partition, tx file is same. Downgrade version
        downgradeTxFile(src, removedPartitions);

        // Act
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);

        // Verify
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Second run of migration should not do anything
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Third time, downgrade and migrate
        downgradeTxFile(src, removedPartitions);
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));
    }

    private void downgradeTxFile(TableModel src, LongList removedPartitions) {
        engine.clear();
        downgradeMetaDataFile(src);

        try (Path path = new Path()) {
            path.concat(root).concat(src.getName()).concat(TableUtils.META_FILE_NAME);
            FilesFacade ff = configuration.getFilesFacade();

            // Read current symbols list
            IntList symbolCounts = new IntList();
            path.trimTo(0).concat(root).concat(src.getName());
            LongList attachedPartitions = new LongList();
            try (TxReader txFile = new TxReader(ff, path.$(), src.getPartitionBy())) {
                txFile.readUnchecked();

                for (int i = 0; i < txFile.getPartitionCount() - 1; i++) {
                    attachedPartitions.add(txFile.getPartitionTimestamp(i));
                    attachedPartitions.add(txFile.getPartitionSize(i));
                }
                txFile.readSymbolCounts(symbolCounts);
            }

            path.trimTo(0).concat(root).concat(src.getName()).concat(TXN_FILE_NAME);
            try (PagedMappedReadWriteMemory rwTx = new PagedMappedReadWriteMemory(ff, path.$(), ff.getPageSize())) {
                rwTx.putInt(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT, symbolCounts.size());
                rwTx.jumpTo(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + 4);

                // Tx file used to have 4 bytes per symbol
                for (int i = 0; i < symbolCounts.size(); i++) {
                    rwTx.putInt(symbolCounts.getQuick(i));
                }

                // and stored removed partitions list
                if (removedPartitions != null) {
                    rwTx.putInt(removedPartitions.size());
                    for (int i = 0; i < removedPartitions.size(); i++) {
                        rwTx.putLong(removedPartitions.getQuick(i));
                    }
                } else {
                    rwTx.putInt(0);
                }
            }

            // and have file _archive in each folder the file size except last partition
            if (src.getPartitionBy() != PartitionBy.NONE) {
                DateFormat partitionFmt = getPartitionDateFmt(src.getPartitionBy());
                StringSink sink = new StringSink();
                for (int i = 0; i < attachedPartitions.size() / 2; i++) {
                    long partitionTs = attachedPartitions.getQuick(i * 2);
                    long partitionSize = attachedPartitions.getQuick(i * 2 + 1);
                    sink.clear();
                    partitionFmt.format(partitionTs, null, null, sink);
                    path.trimTo(0).concat(root).concat(src.getName()).concat(sink).concat("_archive");
                    if (ff.exists(path.$())) {
                        ff.remove(path);
                    }
                    try (PagedMappedReadWriteMemory rwAr = new PagedMappedReadWriteMemory(ff, path.$(), 8)) {
                        rwAr.putLong(partitionSize);
                    }
                }
            }

            setMetadataVersion(src, ff, path, VERSION_TX_STRUCT_UPDATE_1);

            path.trimTo(0).concat(root).concat(UPGRADE_FILE_NAME);
            if (ff.exists(path.$())) {
                ff.remove(path.$());
            }
        }
    }

    private CharSequence executeSql(String sql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        return sink;
    }
}
