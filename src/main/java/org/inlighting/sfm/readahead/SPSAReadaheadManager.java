package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.util.SPSAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SPSAReadaheadManager extends AbstractReadaheadManager{

    private static final Logger LOG = LoggerFactory.getLogger(SPSAReadaheadManager.class);

    // unit is MB
    private final double MIN_READAHEAD_SIZE = 0.2;
    private final double MAX_READAHEAD_SIZE = 2;
    private final double START_READAHEAD_SIZE = 5;
    private final double REAL_START_READAHEAD_SIZE;

    private final SPSAUtil SPSA;

    // 1MB
    private final int START_REMAIN_BYTES = 5*1024*1024;
    private int remainBytes = START_REMAIN_BYTES;
    private long remainStartReadTime = 0;
    private double lastSpeed;
    private boolean lastUsed = false;

    public SPSAReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        super(fs, mergedFilePath);
        SPSA = new SPSAUtil(MIN_READAHEAD_SIZE, MAX_READAHEAD_SIZE, START_READAHEAD_SIZE);
        REAL_START_READAHEAD_SIZE = getReadaheadSize();
        LOG.debug("SPSAReadaheadManager initialized.");
    }

    @Override
    public synchronized int readFully(long position, byte[] b, int off, int len) throws IOException {
        long readPosition = position;
        int readOff = off;
        int needLen = len;

        if (curWindow == null) {
            // only run once.
            LOG.debug("CurWindow didn't existed, create it only once!");

            // 开始对readahead计时
            remainStartReadTime = System.currentTimeMillis();

            curWindow = readahead(position, mb2Bytes(REAL_START_READAHEAD_SIZE));
        }

        while (needLen > 0) {
            LOG.debug(String.format("Read from position: %d", position));
            List<ReadaheadEntity> hitReadaheadList = getHitReadaheadListFromCache(readPosition);
            if (hitReadaheadList.size() > 0) {
                for (ReadaheadEntity readaheadEntity: hitReadaheadList) {
                    int read = readaheadEntity.read(b, readPosition, readOff, needLen);
                    LOG.debug(String.format("Read from cached window, [%d-%d)", readPosition, readPosition+read));
                    needLen = needLen - read;

                    // 减掉成功读取的size
                    remainBytes-=read;
                    checkRemainBytes();

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

                // 减掉成功读取的size
                remainBytes-=read;
                checkRemainBytes();

                if (needLen <= 0) {
                    // return read;
                    // Do not return read, because it may continue the last time read.
                    return len;
                } else {
                    readPosition+=read;
                    readOff+=read;
                    READAHEAD_CACHES.put(curWindow.getStartPosition(), curWindow);
                    double mb = getReadaheadSize();
                    int bytes = mb2Bytes(mb);
                    LOG.debug(String.format("Cal next readahead size %fMB, %dBytes", mb, bytes));
                    curWindow = readahead(curWindow.getStartPosition()+curWindow.getReadaheadLength(), bytes);
                }
            } else {
                LOG.debug("No window hit.");
                READAHEAD_CACHES.put(curWindow.getStartPosition(), curWindow);
                double mb = getReadaheadSize();
                int bytes = mb2Bytes(mb);
                LOG.debug(String.format("Get last readahead size %fMb, %dBytes", mb, bytes));
                curWindow = readahead(readPosition, bytes);
            }
        }

        throw new IOException("readFully failed.");
    }

    private void calLastSpeed(int len, long startTime, long endTime) {
        if (!lastUsed) {
            LOG.error("Never used last speed.");
            return;
        }

        double second = (endTime-startTime) / 1000.0;
        double mb = len / 1024.0 / 1024.0;
        lastSpeed =  (mb / second);
        LOG.info(String.format("Len: %fMb, Seconds: %fSec, Cal speed: %f", mb, second, lastSpeed));
        lastUsed = false;
    }

    private double getReadaheadSize() {
        if (lastUsed) {
            return SPSA.requestLastReadaheadSize();
        } else {
            lastUsed = true;
            return SPSA.requestNextReadaheadSize(-lastSpeed);
        }
    }

    private void checkRemainBytes() {
        if (remainBytes<=0) {
            calLastSpeed(START_REMAIN_BYTES, remainStartReadTime, System.currentTimeMillis());
            remainBytes = START_REMAIN_BYTES;
            remainStartReadTime = System.currentTimeMillis();
        }
    }
}
