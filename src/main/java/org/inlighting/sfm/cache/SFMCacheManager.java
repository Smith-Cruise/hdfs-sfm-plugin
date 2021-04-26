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

    private static final Logger LOG = LoggerFactory.getLogger(SFMCacheManager.class);

    // lock for sfmCache and underlying stream
    private Object lock;

    private SFMCache curSFMCache;

    private SFMCache nextSFMCache;

    private final FSDataInputStream UNDER_LYING_STREAM;

    private int curPrefetchSize = 10240;

//    private boolean shouldLargePrefetchSize = false;
//
//    private int maxPrefetchSize = 20480;

    public SFMCacheManager(FileSystem fs, Path mergedPath) throws IOException {
        // bufferSize no effect.
        this.UNDER_LYING_STREAM = fs.open(mergedPath, 4096);
    }

    public synchronized int read(long position, byte[] b, int off, int len) throws IOException {
        if (curSFMCache != null) {
            if (curSFMCache.isHit(position, len)) {
                LOG.debug(String.format("Hit cache, just return it. {position: %d, len: %d}", position, len));
                int read = curSFMCache.read(b, position, off, len);


                return read;
            } else {
                synchronized (lock) {
                    curSFMCache.close();
                    curSFMCache = null;
                    if (nextSFMCache != null) {
                        curSFMCache = nextSFMCache;
                        nextSFMCache = null;
                    }
                }
            }
        }

//        synchronized (lock) {
//            if (nextSFMCache == null) {
//                if (shouldLargePrefetchSize) {
//                    curPrefetchSize = curPrefetchSize * 2;
//                    curPrefetchSize = Math.min(curPrefetchSize, maxPrefetchSize);
//                }
//                new Fetcher(curSFMCache.getOffsetEnd(), curPrefetchSize).start();
//            }
//        }


//        if (curSFMCache != null && curSFMCache.isHit(position, len)) {
//
//
//            if (curSFMCache.isHit(position, len)) {
//                if (nextSFMCache == null) {
//                    Fetcher fetcher = new Fetcher();
//                }
//
//                // hit, just read
//                LOG.debug(String.format("Hit cache, just return it. {position: %d, len: %d}", position, len));
//                return ;
//            } else {
//                curSFMCache.close();
//                curSFMCache = null;
//            }
//        }



//        if (sfmCache != null) {
//            if (sfmCache.isHit(position, len)) {
//
//            } else {
//                sfmCache.close();
//                sfmCache = null;
//            }
//        }

        // start to read
        // 10mb
//        int prefetchLen = 10 * 1024 * 1024;
//        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(prefetchLen);
//
//        int read = UNDER_LYING_STREAM.read(position, byteBuffer);
//        // put in cache
//        sfmCache = new SFMCache(position, position + read, byteBuffer);
//        LOG.debug(String.format("Create cache, [%d-%d]", position, position+read));
//        return sfmCache.read(b, position, off, len);
        return 0;
    }

    @Override
    public void close() throws IOException {
//        UNDER_LYING_STREAM.close();
//        if (sfmCache != null) {
//            LOG.debug("Invalidate SFM cache, the cache hit rate is " + sfmCache.getHitRate());
//            sfmCache = null;
//        }
    }

    private class Fetcher extends Thread {

        private long position;

        private int length;

        public Fetcher(long position, int length) {
            this.position = position;
            this.length = length;
        }

        @Override
        public void run() {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(length);
            synchronized (lock) {
                try {
                    int read = UNDER_LYING_STREAM.read(position, byteBuffer);
                    nextSFMCache = new SFMCache(position, position + read, byteBuffer);
                    LOG.debug(String.format("Create next cache, [%d-%d]", position, position+read));
                } catch (IOException e) {
                   e.printStackTrace();
                }
            }

        }
    }

    private class SFMCache implements Closeable{
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

        public long getOffsetEnd() {
            return offsetEnd;
        }

        @Override
        public void close() throws IOException {
            byteBuffer = null;
            LOG.debug("Invalidate SFM cache, the cache hit rate is " + getHitRate());
        }
    }
}
