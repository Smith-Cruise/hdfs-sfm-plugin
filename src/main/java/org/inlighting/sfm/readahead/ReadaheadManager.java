package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.readahead.component.ReadaheadComponent;
import org.inlighting.sfm.readahead.component.SPSAComponent;
import org.inlighting.sfm.readahead.component.StaticComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

// ReadaheadManager should created for each sfmBasePath, if enable readahead.
public class ReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReadaheadManager.class);

    private final ReadaheadComponent readaheadComponent;
    private final FSDataInputStream UNDER_LYING_STREAM;

    private ReadaheadEntity trashWindow;
    private ReadaheadEntity curWindow;

    public ReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
//        readaheadComponent = new SPSAComponent();
//        readaheadComponent.initialize(1, 30, 5);
        readaheadComponent = new StaticComponent();
        readaheadComponent.initialize(5,5,5);
        UNDER_LYING_STREAM = fs.open(mergedFilePath);
        LOG.info("Readahead manager create succeed for: " + mergedFilePath.toUri().getPath());
    }

    public synchronized int readFully(long position, byte[] b, int off, int len) throws IOException {
        long readPosition = position;
        int readOff = off;
        int needLen = len;

        if (curWindow == null) {
            // only run once.
            LOG.debug("Initialize cur & ahead window");

            int readaheadSizeMB = readaheadComponent.requestNextReadaheadSize(1000);
            int readaheadSizeBytes = mb2Byte(readaheadSizeMB);
            curWindow = readahead(position, readaheadSizeBytes);
        }

        while (needLen > 0) {
            LOG.debug(String.format("Need to read from readPosition: %d", position));
            if (trashWindow != null && trashWindow.hit(readPosition)) {
                int read = trashWindow.read(b, readPosition, readOff, needLen);
                LOG.debug(String.format("Read from trashWindow, [%d-%d)", readPosition, readPosition+read));
                needLen = needLen - read;
                if (needLen <= 0) {
                    // return read;
                    // Do not return read, because it may continue the last time read.
                    return len;
                } else {
                    readPosition+=read;
                    readOff+=read;
                }
            } else if (curWindow.hit(readPosition)) {
                int read = curWindow.read(b, readPosition, readOff, needLen);
                LOG.debug(String.format("Read from curWindow, [%d-%d)", readPosition, readPosition+read));
                needLen = needLen - read;
                if (needLen <= 0) {
                    // return read;
                    // Do not return read, because it may continue the last time read.
                    return len;
                } else {
                    readPosition+=read;
                    readOff+=read;
                    double lastHitSpend = curWindow.getHitSpend();
                    if (trashWindow != null) {
                        LOG.debug(String.format("Drop trashWindow, %s", trashWindow));
                    }
                    trashWindow = curWindow;
                    int readaheadSizeMB = readaheadComponent.requestNextReadaheadSize(lastHitSpend);
                    int readaheadSizeBytes = mb2Byte(readaheadSizeMB);
                    curWindow = readahead(trashWindow.getStartPosition()+trashWindow.getReadaheadLength(), readaheadSizeBytes);
                }
            } else {
                // curWindow & trashWindow both not hit.
                // invalid trashWindow & curWindow
                LOG.debug("curWindow & trashWindow both not hit.");
                if (trashWindow != null) {
                    LOG.debug(String.format("Drop trashWindow, %s", trashWindow));
                }
                trashWindow = curWindow;
                int readaheadSizeMB = readaheadComponent.requestLastReadaheadSize();
                int readaheadSizeBytes = mb2Byte(readaheadSizeMB);
                LOG.debug(String.format("Get last readahead size %dBytes", readaheadSizeBytes));
                curWindow = readahead(readPosition, readaheadSizeBytes);
            }
        }

        return len;
    }

    private ReadaheadEntity readahead(long startPosition, int size) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        // todo
//        UNDER_LYING_STREAM.setReadahead((long) size);
        LOG.debug(String.format("Readahead get [%d, %d) size:%dBytes, size:%fKb, size:%fMb", startPosition, startPosition+size,
                size, (double) size / 1024, (double) size / 1024 / 1024));
        long start = System.currentTimeMillis();
        int read = UNDER_LYING_STREAM.read(startPosition, byteBuffer);
        long end = System.currentTimeMillis();
        return new ReadaheadEntity(startPosition, read, (int)(end-start) ,byteBuffer);
    }

    private int mb2Byte(int mb) {
        return mb * 1024 * 1024;
    }

}

