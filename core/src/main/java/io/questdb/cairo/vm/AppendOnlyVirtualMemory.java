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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class AppendOnlyVirtualMemory extends PagedVirtualMemory implements MappedReadWriteMemory {
    private static final Log LOG = LogFactory.getLog(AppendOnlyVirtualMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;
    private int mappedPage;

    public AppendOnlyVirtualMemory(FilesFacade ff, LPSZ name, long pageSize) {
        of(ff, name, pageSize);
    }

    public AppendOnlyVirtualMemory() {
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public long getPageAddress(int page) {
        if (page == mappedPage) {
            return pageAddress;
        }
        return 0L;
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    @Override
    protected void release(int page, long address) {
        ff.munmap(address, getPageSize(page));
    }

    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                VmUtils.bestEffortClose(ff, LOG, fd, truncate, sz, getMapPageSize());
            } finally {
                fd = -1;
            }
        }
    }

    public void ensureFileSize(int page) {
        long target = pageOffset(page + 1);
        if (ff.length(fd) < target && !ff.allocate(fd, target)) {
            throw CairoException.instance(ff.errno()).put("Appender resize failed fd=").put(fd).put(", size=").put(target);
        }
    }

    @Override
    public void growToFileSize() {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() {
        return fd == -1;
    }

    public long mapPage(int page) {
        ensureFileSize(page);
        long offset = pageOffset(page);
        long address = ff.mmap(fd, getMapPageSize(), offset, Files.MAP_RW);
        if (address != -1) {
            mappedPage = page;
            return address;
        }
        mappedPage = -1;
//        new Exception("could not mmap").printStackTrace();
        throw CairoException.instance(ff.errno()).put("could not mmap for append fd=").put(fd).put(", offset=").put(offset).put(", size=").put(getMapPageSize());
    }

    public long getAppendAddress() {
        long appendOffset = getAppendOffset();
        return getPageAddress(pageIndex(appendOffset)) + offsetInPage(appendOffset);
    }

    public long getAppendAddressSize() {
        long appendOffset = getAppendOffset();
        long sz = getPageSize(pageIndex(appendOffset));
        return sz - offsetInPage(appendOffset);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
        // size of file does not mapper for mapping file for append
        of(ff, name, pageSize);
    }

    public final void of(FilesFacade ff, LPSZ name, long pageSize) {
        close();
        this.ff = ff;
        mappedPage = -1;
        setPageSize(pageSize);
        fd = TableUtils.openFileRWOrFail(ff, name);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(pageSize).$(']').$();
    }

    @Override
    public boolean isDeleted() {
        return !ff.exists(fd);
    }

    public long getFd() {
        return fd;
    }

    public final void of(FilesFacade ff, long fd, long pageSize) {
        close();
        this.ff = ff;
        setPageSize(pageSize);
        this.fd = fd;
    }

    public final void setSize(long size) {
        jumpTo(size);
    }

    public void sync(boolean async) {
        if (pageAddress != 0) {
            if (ff.msync(pageAddress, getMapPageSize(), async) == 0) {
                return;
            }
            LOG.error().$("could not msync [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }

        releaseCurrentPage();
        if (!ff.truncate(Math.abs(fd), getMapPageSize())) {
            throw CairoException.instance(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getMapPageSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    static void bestEffortTruncate(FilesFacade ff, Log log, long fd, long size, long mapPageSize) {
        if (ff.truncate(Math.abs(fd), size)) {
            log.debug()
                    .$("truncated and closed [fd=").$(fd)
                    .$(", size=").$(size)
                    .$(']').$();
        } else {
            if (ff.isRestrictedFileSystem()) {
                // Windows does truncate file if it has a mapped page somewhere, could be another handle and process.
                // To make it work size needs to be rounded up to nearest page.
                long n = (size - 1) / mapPageSize;
                if (ff.truncate(Math.abs(fd), (n + 1) * mapPageSize)) {
                    log.debug()
                            .$("truncated and closed, second attempt [fd=").$(fd)
                            .$(", size=").$((n + 1) * mapPageSize)
                            .$(']').$();
                    return;
                }
            }
            log.debug().$("closed without truncate [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    FilesFacade getFilesFacade() {
        return ff;
    }

    void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(0, pageAddress);
            pageAddress = 0;
        }
    }
}
