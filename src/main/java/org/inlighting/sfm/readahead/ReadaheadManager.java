package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

// ReadaheadManager should created for each sfmBasePath, if enable readahead.
public class ReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReadaheadManager.class);

    private final FSDataInputStream UNDER_LYING_STREAM;

    private ReadaheadEntity curReadaheadEntity = null;

    public ReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        UNDER_LYING_STREAM = fs.open(mergedFilePath);
        LOG.info("Readahead manager create succeed for: " + mergedFilePath.toUri().getPath());
    }

    public synchronized int read(long position, byte[] b, int off, int len) throws IOException {
        if (curReadaheadEntity != null) {
            if (curReadaheadEntity.isHit(position, len)) {
                // hit, just read
                LOG.debug(String.format("Hit cache, just return it. {position: %d, len: %d}", position, len));
                return curReadaheadEntity.read(b, position, off, len);
            } else {
                LOG.debug("Invalidate SFM cache, the cache hit rate is " + curReadaheadEntity.getHitRate());
                curReadaheadEntity = null;
            }
        }

        // start to readahead
        // 10mb
        int readaheadLen = 10 * 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(readaheadLen);

        int read = UNDER_LYING_STREAM.read(position, byteBuffer);
        // put in cache
        curReadaheadEntity = new ReadaheadEntity(position, read, byteBuffer);
        LOG.debug(String.format("Create cache, [%d-%d]", position, position+read));
        return curReadaheadEntity.read(b, position, off, len);
    }

    class ReadaheadEntity {
        private long startOffset;

        private long readaheadLength;

        private ByteBuffer byteBuffer;

        private int read = 0;

        private int hit = 0;

        public ReadaheadEntity(long offset, long length, ByteBuffer byteBuffer) {
            this.startOffset = offset;
            this.readaheadLength = length;
            this.byteBuffer = byteBuffer;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getReadaheadLength() {
            return readaheadLength;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public boolean isHit(long offset, long length) {
            read++;
            if ((offset >= startOffset) && ((offset+length) <= (startOffset+readaheadLength))) {
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
            byteBuffer.position((int)(position - startOffset));
            int tmpStart = byteBuffer.position();
            byteBuffer.get(b, offset, len);
            int tmpEnd = byteBuffer.position();
            return tmpEnd - tmpStart;
        }
    }
}
