package org.inlighting.sfm.readahead;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.util.LruCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractReadaheadManager implements ReadaheadManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractReadaheadManager.class);

    private final FSDataInputStream UNDER_LYING_STREAM;

    protected ReadaheadEntity curWindow;
    protected final LruCache<Long, ReadaheadEntity> READAHEAD_CACHES;

    public AbstractReadaheadManager(FileSystem fs, Path mergedFilePath) throws IOException {
        UNDER_LYING_STREAM = fs.open(mergedFilePath);
        READAHEAD_CACHES = new LruCache<>(SFMConstants.READAHEAD_CACHE_NUM);
    }

    @Override
    public ReadaheadEntity readahead(long startPosition, int size) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        // todo
//        UNDER_LYING_STREAM.setReadahead((long) size);
        LOG.debug(String.format("Readahead get [%d, %d) size:%dBytes, size:%fKb, size:%fMb", startPosition, startPosition+size,
                size, (double) size / 1024, (double) size / 1024 / 1024));
        long start = System.currentTimeMillis();
//        UNDER_LYING_STREAM.setReadahead((long) size);
        int read = UNDER_LYING_STREAM.read(startPosition, byteBuffer);
        long end = System.currentTimeMillis();
        return new ReadaheadEntity(startPosition, read, (int)(end-start) ,byteBuffer);
    }

    protected int mb2Bytes(double mb) {
        return (int) Math.round(mb*1024*1024);
    }

    protected List<ReadaheadEntity> getHitReadaheadListFromCache(long startPosition) {
        List<Long> list = new LinkedList<>();
        for (Map.Entry<Long, ReadaheadEntity> entry: READAHEAD_CACHES.entrySet()) {
            ReadaheadEntity entity = entry.getValue();
            if (entity.hit(startPosition)) {
                list.add(entry.getKey());
            }
        }
        List<ReadaheadEntity> res = new LinkedList<>();
        for (Long l: list) {
            res.add(READAHEAD_CACHES.get(l));
        }
        Collections.sort(res);
        return res;
    }
}
