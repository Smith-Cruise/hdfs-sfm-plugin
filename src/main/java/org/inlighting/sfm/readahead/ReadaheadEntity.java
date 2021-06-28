package org.inlighting.sfm.readahead;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadaheadEntity {

    // position in merged file
    private final long startPosition;

    private final int readaheadLength;

    // mill second
    private final int fetchTime;

    private int used;

    private final ByteBuffer byteBuffer;

    public ReadaheadEntity(long startPosition, int readaheadLength, int fetchTime, ByteBuffer byteBuffer) {
        this.startPosition = startPosition;
        this.readaheadLength = readaheadLength;
        this.fetchTime = fetchTime;
        this.byteBuffer = byteBuffer;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public int getReadaheadLength() {
        return readaheadLength;
    }

    public boolean hit(long position) {
        return position >= startPosition && position < startPosition + readaheadLength;
    }

    // return in %
    public double getHitRate() {
        return (double) used / (double) readaheadLength * 100;
    }

    public double getHitSpend() {
        return (double) used / (double) fetchTime;
    }

    public int read(byte[] b, long position, int offset, int len) throws IOException {
        byteBuffer.position((int)(position - startPosition));
        int remain = byteBuffer.remaining();
        if (len <= remain) {
            byteBuffer.get(b, offset, len);
            used += len;
            return len;
        } else {
            byteBuffer.get(b, offset, remain);
            used += remain;
            return remain;
        }
    }


}
