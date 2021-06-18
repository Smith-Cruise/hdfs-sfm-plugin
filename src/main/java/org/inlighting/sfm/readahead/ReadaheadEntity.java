package org.inlighting.sfm.readahead;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadaheadEntity {
    private int read = 0;

    private int hit = 0;

    // position in merged file
    private final long startPosition;

    private final int readaheadLength;

    private final ByteBuffer byteBuffer;

    public ReadaheadEntity(long startPosition, int readaheadLength, ByteBuffer byteBuffer) {
        this.startPosition = startPosition;
        this.readaheadLength = readaheadLength;
        this.byteBuffer = byteBuffer;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public int getReadaheadLength() {
        return readaheadLength;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public boolean hit(long position) {
        read++;
        if (position >= startPosition && position < startPosition+readaheadLength) {
            hit++;
            return true;
        } else {
            return false;
        }
    }

    public float getHitRate() {
        return (float) hit / read;
    }

    public int read(byte[] b, long position, int offset, int len) throws IOException {
        byteBuffer.position((int)(position - startPosition));
        int remain = byteBuffer.remaining();
        if (len <= remain) {
            byteBuffer.get(b, offset, len);
            return len;
        } else {
            byteBuffer.get(b, offset, remain);
            return remain;
        }
    }


}
