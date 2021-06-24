package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.readahead.component.ReadaheadComponent;
import org.inlighting.sfm.readahead.component.ReadaheadLock;
import org.inlighting.sfm.readahead.component.SPSAComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

// ReadaheadManager should created for each sfmBasePath, if enable readahead.
public class ReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReadaheadManager.class);

    private final ReadaheadComponent readaheadComponent;
    private final FSDataInputStream UNDER_LYING_STREAM;

    private final ReadaheadLock FETCHER_LOCK = new ReadaheadLock();
    private final ReadaheadLock FETCHER_RUNNING_LOCK = new ReadaheadLock();

    private ReadaheadEntity curWindow;
    private ReadaheadEntity aheadWindow;

    public ReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        readaheadComponent = new SPSAComponent();
        readaheadComponent.initialize(10, 50, 10);
        UNDER_LYING_STREAM = fs.open(mergedFilePath);
        LOG.info("Readahead manager create succeed for: " + mergedFilePath.toUri().getPath());
    }


    public synchronized int readFully(long position, byte[] b, int off, int len) throws IOException {
        if (curWindow == null) {
            // init window
            LOG.debug("Initialize cur & ahead window");

            int readaheadSizeMB = readaheadComponent.requestNextReadaheadSize();
            int readaheadSizeBytes = mb2Byte(readaheadSizeMB);
            curWindow = readahead(position, readaheadSizeBytes);
            triggerAsyncReadahead(position+readaheadSizeBytes, readaheadSizeBytes);
        }

        long readPosition = position;
        int readOff = off;
        int needLen = len;
        while (needLen > 0) {
            if (curWindow.hit(readPosition)) {
//                LOG.debug(String.format("Hit in curWindow, position: %d", readPosition));
                int read = curWindow.read(b, readPosition, readOff, needLen);
                needLen = needLen - read;
                if (needLen <= 0) {
                    // return read;
                    // Do not return read, because it may continue the last time read.
                    return len;
                } else {
                    readPosition+=read;
                    readOff+=read;
                    // check fetcher is not running first, make sure it existed ahead window
                    waitFetcherStop();
                    // need bytes from aheadWindow
                    FETCHER_LOCK.lock();
                    // get curWindow hit rate before release it
                    double lastHitRate = curWindow.getHitRate();
                    curWindow = aheadWindow;
                    aheadWindow = null;
                    FETCHER_LOCK.unlock();

                    // async readahead
                    triggerAsyncReadahead(curWindow.getStartPosition()+curWindow.getReadaheadLength(), lastHitRate);
                }
            } else {
                // check aheadWindow, first need to acquire lock
                waitFetcherStop();
                FETCHER_LOCK.lock();
                if (aheadWindow != null && aheadWindow.hit(readPosition)) {
                    LOG.debug(String.format("Hit in aheadWindow, position: %d", readPosition));
                    // release curWindow
                    double lastHitRate = curWindow.getHitRate();
                    curWindow = aheadWindow;
                    aheadWindow = null;
                    FETCHER_LOCK.unlock();
                    // async readahead
                    triggerAsyncReadahead(curWindow.getStartPosition()+curWindow.getReadaheadLength(), lastHitRate);
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
                    FETCHER_LOCK.unlock();
                    readaheadComponent.reInitialize();
                    readFully(position, b, off, len);
                }

            }
        }
        return len;
    }

    // use Readahead component
    private void triggerAsyncReadahead(long startPosition, double lastHitRate){
        int lengthMb = readaheadComponent.requestNextReadaheadSize(lastHitRate);
        int lengthBytes = mb2Byte(lengthMb);
        FETCHER_RUNNING_LOCK.lock();
        new Thread(new Fetcher(startPosition, lengthBytes)).start();
    }

    private void triggerAsyncReadahead(long startPosition, int length){
        FETCHER_RUNNING_LOCK.lock();
        new Thread(new Fetcher(startPosition, length)).start();
    }

    private ReadaheadEntity readahead(long startPosition, int size) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        // todo
//        UNDER_LYING_STREAM.setReadahead((long) size);
        LOG.debug(String.format("Readahead get [%d, %d) size:%dBytes", startPosition, startPosition+size, size));
        int read = UNDER_LYING_STREAM.read(startPosition, byteBuffer);
        return new ReadaheadEntity(startPosition, read, byteBuffer);
    }

    private void waitFetcherStop() {
        // check fetcher is not running first, make sure it existed ahead window
        FETCHER_RUNNING_LOCK.lock();
        FETCHER_RUNNING_LOCK.unlock();
    }

    private int mb2Byte(int mb) {
        return mb * 1024 * 1024;
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
            try {
                ReadaheadEntity tmp = readahead(startPosition, size);
                FETCHER_LOCK.lock();
                aheadWindow = tmp;
                FETCHER_LOCK.unlock();
            } catch (IOException e) {
                LOG.error(String.format("Readahead fetcher fetch failed. [%d-%d)", startPosition, startPosition+size), e);
            }
            LOG.debug(String.format("Async readahead fetch succeed! [%d-%d)", startPosition, startPosition+size));
            FETCHER_RUNNING_LOCK.unlock();
        }
    }
}

