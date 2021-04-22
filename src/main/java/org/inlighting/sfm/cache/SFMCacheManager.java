package org.inlighting.sfm.cache;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SFMCacheManager implements Closeable {

    private SFMCache sfmCache;

    private final FSDataInputStream UNDER_LYING_STREAM;

    private static final Logger LOG = LoggerFactory.getLogger(SFMCacheManager.class);

    public SFMCacheManager(FileSystem fs, Path mergedPath) throws IOException {
        // bufferSize no effect.
        this.UNDER_LYING_STREAM = fs.open(mergedPath, 4096);
    }

    public synchronized int read(long position, byte[] b, int off, int len) throws IOException {
        if (sfmCache != null) {
            if (sfmCache.isHit(position, len)) {
                // hit, just read
                LOG.debug(String.format("Hit cache, just return it. {position: %d, len: %d}", position, len));
                return sfmCache.read(b, position, off, len);
            } else {
                LOG.debug("Invalidate SFM cache, the cache hit rate is " + sfmCache.getHitRate());
                sfmCache = null;
            }
        }

        // start to read
        // 10mb
        int prefetchLen = 10 * 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(prefetchLen);

        int read = UNDER_LYING_STREAM.read(position, byteBuffer);
        // put in cache
        sfmCache = new SFMCache(position, position + read, byteBuffer);
        LOG.debug(String.format("Create cache, [%d-%d]", position, position+read));
        return sfmCache.read(b, position, off, len);
    }



    @Override
    public void close() throws IOException {
        UNDER_LYING_STREAM.close();
        LOG.debug("Invalidate SFM cache, the cache hit rate is " + sfmCache.getHitRate());
        sfmCache = null;
    }

    private class SFMCache {
        private long offsetStart;

        private long offsetEnd;

        private ByteBuffer byteBuffer;

        private int read = 0;

        private int hit = 0;

        public SFMCache(long offsetStart, long offsetEnd, ByteBuffer byteBuffer) {
            this.offsetStart = offsetStart;
            this.offsetEnd = offsetEnd;
            this.byteBuffer = byteBuffer;
        }

        public boolean isHit(long offset, long length) {
            read++;
            if ((offset >= offsetStart) && ((offset + length) <= offsetEnd)) {
                hit ++;
                return true;
            } else {
                return false;
            }
        }

        public float getHitRate() {
            return (float) hit / read;
        }

        public int read(byte[] b, long position, int offset, int len) throws IOException {
            byteBuffer.position((int)(position - offsetStart));
            int tmpStart = byteBuffer.position();
            byteBuffer.get(b, offset, len);
            int tmpEnd = byteBuffer.position();
            return tmpEnd - tmpStart;
        }
    }
}
