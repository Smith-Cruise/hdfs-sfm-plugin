package org.inlighting.sfm.fs;

import org.apache.hadoop.fs.*;
import org.inlighting.sfm.readahead.ReadaheadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;


// like DFSInputStream
public class SFMFsInputStream extends FSInputStream implements CanSetDropBehind, CanSetReadahead {

    private static final Logger LOG = LoggerFactory.getLogger(SFMFsInputStream.class);

    private long mergedPosition;
    private final long mergedStart, mergedEnd;

    private final byte[] ONE_BYTE_BUFFER = new byte[1];

    private final FSDataInputStream UNDER_LYING_STREAM;
    private final ReadaheadManager READAHEAD_MANAGER;

    public SFMFsInputStream(FileSystem fs, Path mergedPath, long start, long length, int bufferSize, ReadaheadManager readaheadManager) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException(String.format("Negative length: %d", length));
        }

        if (readaheadManager == null) {
            UNDER_LYING_STREAM = fs.open(mergedPath, bufferSize);
            UNDER_LYING_STREAM.seek(start);
            READAHEAD_MANAGER = null;
        } else {
            // Do not create underLyingStream, use cacheManager only.
            READAHEAD_MANAGER = readaheadManager;
            UNDER_LYING_STREAM = null;
        }

        mergedStart = start;
        mergedEnd = start + length;
        mergedPosition = start;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized int available() throws IOException {
        final long remaining = mergedEnd - mergedPosition;
        return remaining <= Integer.MAX_VALUE? (int) remaining: Integer.MAX_VALUE;
    }

    @Override
    public synchronized void close() throws IOException {
        if (! isEnableCache()) {
            UNDER_LYING_STREAM.close();
        }
        super.close();
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException("mark not implemented");
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("reset not implemented");
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }

        int ret = -1;
        if (len == 0) {
            return ret;
        }
        int newLen = len;
        if (mergedPosition + len > mergedEnd) {
            newLen = (int) (mergedEnd - mergedPosition);
        }

        // end case, end = position
        if (newLen == 0) {
            return ret;
        }

        // start to read
        if (isEnableCache()) {
            ret = READAHEAD_MANAGER.read(mergedPosition, b, off, newLen);
        } else {
            ret = UNDER_LYING_STREAM.read(b,off, newLen);
        }
        mergedPosition += ret;
        return ret;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        long tmpN = n;
        if (tmpN > 0) {
            final long actualRemaining = mergedEnd - mergedPosition;
            if (tmpN > actualRemaining) {
                tmpN = actualRemaining;
            }
            if (! isEnableCache()) {
                UNDER_LYING_STREAM.seek(tmpN + mergedPosition);
            }
            mergedPosition += tmpN;
            return tmpN;
        }
        // NB: the contract is described in java.io.InputStream.skip(long):
        // this method returns the number of bytes actually skipped, so,
        // the return value should never be negative.
        return 0;
    }

    // position readable, do not change current offset.
    // maybe error.
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        int nLength = length;
        if (mergedStart + nLength + position > mergedEnd) {
            nLength = (int) (mergedEnd - mergedStart - position);
        }
        if (nLength <= 0) {
            return -1;
        }

        if (isEnableCache()) {
            return READAHEAD_MANAGER.read(position + mergedStart, buffer, offset, length);
        } else {
            return UNDER_LYING_STREAM.read(position + mergedStart, buffer, offset, length);
        }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        validatePositionedReadArgs(position, buffer, offset, length);
        if (length == 0) {
            return;
        }
        if (mergedStart + length + position > mergedEnd) {
            throw new EOFException("Not enough bytes to read.");
        }

        if (isEnableCache()) {
            READAHEAD_MANAGER.read(mergedStart + position, buffer, offset, length);
        } else {
            UNDER_LYING_STREAM.readFully(mergedStart + position, buffer, offset, length);
        }
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
        UNDER_LYING_STREAM.setDropBehind(dropCache);
    }

    @Override
    public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
        UNDER_LYING_STREAM.setReadahead(readahead);
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        validatePosition(pos);
        mergedPosition = mergedStart + pos;
        if (! isEnableCache()) {
            UNDER_LYING_STREAM.seek(mergedPosition);
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        return (mergedPosition - mergedStart);
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        // do not need to implement this
        // hdfs in itself does seektonewsource
        // while reading.
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        int ret = read(ONE_BYTE_BUFFER, 0, 1);
        return (ret <= 0) ? -1: (ONE_BYTE_BUFFER[0] & 0xff);
    }

    private boolean isEnableCache() {
        return READAHEAD_MANAGER != null;
    }

    private void validatePosition(final long pos) throws IOException {
        if (pos < 0) {
            throw new IOException(String.format("Negative position: %d", pos));
        }
        final long length = mergedEnd - mergedStart;
        if (pos > length) {
            throw new IOException(String.format("Position %d larger than length %d", pos, length));
        }
    }
}
