package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// ReadaheadManager should created for each sfmBasePath, if enable readahead.
public class ReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReadaheadManager.class);

    // 1mb
    private final int MIN_READAHEAD_SIZE = 1024 * 1024;
    // 10mb
    private final int MAX_READAHEAD_SIZE = 50*1024*1024;

    private int lastReadaheadSize;

    private final FSDataInputStream UNDER_LYING_STREAM;
    private ReadaheadEntity curWindow;
    private ReadaheadEntity aheadWindow;

    private final Lock FETCHER_LOCK = new ReentrantLock();


    public ReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        UNDER_LYING_STREAM = fs.open(mergedFilePath);
        LOG.info("Readahead manager create succeed for: " + mergedFilePath.toUri().getPath());
    }


    public synchronized int readFully(long position, byte[] b, int off, int len) throws IOException {
        if (curWindow == null) {
            // init window
            LOG.debug("Initialize cur & ahead window");
            // first read size, 1MB <= size*10 <= 10MB
            lastReadaheadSize = len*10;
            lastReadaheadSize = Math.max(MIN_READAHEAD_SIZE, lastReadaheadSize);
            lastReadaheadSize = Math.min(lastReadaheadSize, MAX_READAHEAD_SIZE);

            ReadaheadEntity readaheadEntity = readahead(position, lastReadaheadSize);
            triggerAsyncReadahead(position+lastReadaheadSize);
            curWindow = readaheadEntity;
        }

        long readPosition = position;
        int readOff = off;
        int needLen = len;
        while (needLen > 0) {
            if (curWindow.hit(readPosition)) {
                LOG.debug(String.format("Hit in curWindow, position: %d", readPosition));
                int read = curWindow.read(b, readPosition, readOff, needLen);
                needLen = needLen - read;
                if (needLen <= 0) {
                    return read;
                } else {
                    readPosition+=read;
                    readOff+=read;
                    // need bytes from aheadWindow
                    FETCHER_LOCK.lock();
                    curWindow = aheadWindow;
                    aheadWindow = null;
                    FETCHER_LOCK.unlock();

                    // async readahead
                    triggerAsyncReadahead(curWindow.getStartPosition()+curWindow.getReadaheadLength());
                }
            } else {
                // check aheadWindow, first need to acquire lock
                FETCHER_LOCK.lock();
                if (aheadWindow != null && aheadWindow.hit(readPosition)) {
                    LOG.debug(String.format("Hit in aheadWindow, position: %d", readPosition));
                    curWindow = aheadWindow;
                    aheadWindow = null;
                    FETCHER_LOCK.unlock();
                    // async readahead
                    triggerAsyncReadahead(curWindow.getStartPosition()+curWindow.getReadaheadLength());
                } else {
                    // curWindow & aheadWindow both not hit.
                    // invalid curWindow & aheadWindow
                    if (curWindow != null) {
                        LOG.debug(String.format("Invalidate curWindow, hit rate: %f", curWindow.getHitRate()));
                        curWindow = null;
                    }
                    if (aheadWindow != null) {
                        LOG.debug(String.format("Invalidate aheadWindow, hit rate: %f", aheadWindow.getHitRate()));
                        aheadWindow = null;
                    }
                    lastReadaheadSize = 0;
                    FETCHER_LOCK.unlock();

                    readFully(position, b, off, len);
                }

            }
        }
        return len;
    }

    private void triggerAsyncReadahead(long startPosition) throws IOException {
        int readaheadSize = Math.max(MIN_READAHEAD_SIZE, lastReadaheadSize*2);
        readaheadSize = Math.min(readaheadSize, MAX_READAHEAD_SIZE);
        triggerAsyncReadahead(startPosition, readaheadSize);
    }

    private void triggerAsyncReadahead(long startPosition, int length) throws IOException {
        new Thread(new Fetcher(startPosition, length)).start();
    }

    private ReadaheadEntity readahead(long startPosition, int size) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
//        UNDER_LYING_STREAM.setReadahead((long) size);
        int read = UNDER_LYING_STREAM.read(startPosition, byteBuffer);
        return new ReadaheadEntity(startPosition, read, byteBuffer);
    }

    private class Fetcher implements Runnable {

        private final long startPosition;

        // not support to large size
        private final int size;

        private Fetcher(long startPosition, int size) {
            this.startPosition = startPosition;
            this.size = size;
        }

        @Override
        public void run() {
            FETCHER_LOCK.lock();
            try {
                aheadWindow = readahead(startPosition, size);
            } catch (IOException e) {
               LOG.error(String.format("Readahead fetcher fetch failed. [%d-%d)", startPosition, startPosition+size), e);
            }
            LOG.info(String.format("Async readahead fetch succeed! [%d-%d)", startPosition, startPosition+size));
            FETCHER_LOCK.unlock();
        }
    }
}