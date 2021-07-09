package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class StaticReadaheadManager extends AbstractReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(StaticReadaheadManager.class);

    private final double STATIC_READAHEAD_SIZE = 0.5;

    public StaticReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        super(fs, mergedFilePath);
        LOG.debug("StaticReadaheadManager initialized. Static size is "+STATIC_READAHEAD_SIZE+"Mb.");
    }

    @Override
    public synchronized int readFully(long position, byte[] b, int off, int len) throws IOException {
        long readPosition = position;
        int readOff = off;
        int needLen = len;

        if (curWindow == null) {
            // only run once.
            LOG.debug("CurWindow didn't existed, create it only once!");
            curWindow = readahead(position, mb2Bytes(STATIC_READAHEAD_SIZE));
        }

        while (needLen > 0) {
            LOG.debug(String.format("Read from position: %d", position));
            List<ReadaheadEntity> hitReadaheadList = getHitReadaheadListFromCache(readPosition);
            if (hitReadaheadList.size() > 0) {
                for (ReadaheadEntity readaheadEntity: hitReadaheadList) {
                    int read = readaheadEntity.read(b, readPosition, readOff, needLen);
                    LOG.debug(String.format("Read from cached window, [%d-%d)", readPosition, readPosition+read));
                    needLen = needLen - read;
                    if (needLen <= 0) {
                        // return read;
                        // Do not return read, because it may continue the last time read.
                        return len;
                    } else {
                        readPosition+=read;
                        readOff+=read;
                    }
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
                    READAHEAD_CACHES.put(curWindow.getStartPosition(), curWindow);
                    double mb = STATIC_READAHEAD_SIZE;
                    int bytes = mb2Bytes(mb);
                    LOG.debug(String.format("Get static readahead size %fMB, %dBytes", mb, bytes));
                    curWindow = readahead(curWindow.getStartPosition()+curWindow.getReadaheadLength(), bytes);
                }
            } else {
                LOG.debug("No window hit.");
                READAHEAD_CACHES.put(curWindow.getStartPosition(), curWindow);
                double mb = STATIC_READAHEAD_SIZE;
                int bytes = mb2Bytes(mb);
                LOG.debug(String.format("Get static readahead size %fMb, %dBytes", mb, bytes));
                curWindow = readahead(readPosition, bytes);
            }
        }

        throw new IOException("readFully failed.");
    }


}
