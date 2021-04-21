package org.inlighting.sfm.cache;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class SFMCacheManager {

    private final FileSystem FS;

    private final List<SFMCache> SFM_CACHE_LIST;

    public SFMCacheManager(FileSystem fs) {
        this.FS = fs;
        SFM_CACHE_LIST = new LinkedList<>();
    }

    public synchronized int read(byte[] b, int off, int len) throws IOException {
        return 0;
    }








    private class SFMCache {
        private long offsetStart;

        private long offsetEnd;

        private ByteBuffer byteBuffer;

        public SFMCache(long offsetStart, long offsetEnd, ByteBuffer byteBuffer) {
            this.offsetStart = offsetStart;
            this.offsetEnd = offsetEnd;
            this.byteBuffer = byteBuffer;
        }

        public boolean isHit(long offset, long length) {
            return (offset >= offsetStart) && ((offset + length) <= offsetEnd);
        }
    }
}
