package org.inlighting.sfm.index;

import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.proto.BloomFilterProtos;
import org.inlighting.sfm.proto.KVsProtos;
import org.inlighting.sfm.proto.TrailerProtos;
import org.inlighting.sfm.util.BloomFilter;
import org.inlighting.sfm.util.BloomFilterIO;
import org.inlighting.sfm.util.SFMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class SFMIndexBuilder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SFMIndexBuilder.class);

    private final FileSystem FS;

    private final String SFM_BASE_PATH;

    private int numIndex = 0;

    // bloom filter
    private static final double FPP = 0.05;

    // trailer info

    private String minKey;

    private String maxKey;

    private String lastKey;

    private long lastNanoTime;

    // KV
    private final List<KV> INDEX_LIST;

    public SFMIndexBuilder(FileSystem fs, String sfmBasePath, String mergeFilename) {
        FS = fs;
        SFM_BASE_PATH = sfmBasePath;
        INDEX_LIST = new ArrayList<>();
    }

    public static SFMIndexBuilder build(FileSystem fs, String sfmBasePath, String mergeFilename) {
        return new SFMIndexBuilder(fs, sfmBasePath, mergeFilename);
    }

    public void add(String filename, long offset, int length, long modificationTime, long nanoTime) throws IOException {
//        checkKey(filename, nanoTime);

        INDEX_LIST.add(new KV(filename, offset, length, modificationTime, false, nanoTime));
        numIndex++;
    }

    public void addDelete(String filename, long modificationTime, long nanoTime) throws IOException {
//        checkKey(filename, nanoTime);
        INDEX_LIST.add(new KV(filename, 0, 0,modificationTime, true, nanoTime));
        numIndex++;
    }

    public int getContainNumOfIndex() {
        return numIndex;
    }

    @Override
    public void close() throws IOException {
        // check index file is existed
        Path masterIndexPath = new Path(SFM_BASE_PATH + "/" + SFMConstants.MASTER_INDEX_NAME);
        Path indexPath = new Path(SFM_BASE_PATH + "/" + SFMConstants.INDEX_NAME);
        FSDataOutputStream indexOutput;
        FSDataOutputStream masterIndexOutput;
        if (FS.exists(indexPath) && FS.exists(masterIndexPath)) {
            masterIndexOutput = FS.append(masterIndexPath);
            indexOutput = FS.append(indexPath);
            LOG.debug(String.format("Index: %s already existed. Append it.", indexPath.toUri().getPath()));
        } else {
            masterIndexOutput = FS.create(masterIndexPath);
            indexOutput = FS.create(indexPath);
        }
        FileStatus indexStatus = FS.getFileStatus(indexPath);

        KVsProtos.KVs.Builder kvsBuilder = KVsProtos.KVs.newBuilder();
        BloomFilter bloomFilter = new BloomFilter(numIndex, FPP);

        // sort first.
        Collections.sort(INDEX_LIST);
        for (KV kv : INDEX_LIST) {
            checkKey(kv.getFilename(), kv.getNanoTime());
            KVsProtos.KV.Builder kvBuilder = KVsProtos.KV.newBuilder();
            kvBuilder.setFilename(kv.getFilename());
            kvBuilder.setModificationTime(kv.getModificationTime());
            if (!kv.isTombstone()) {
                kvBuilder.setOffset(kv.getOffset());
                kvBuilder.setLength(kv.getLength());
                kvBuilder.setTombstone(false);
            } else {
                kvBuilder.setTombstone(true);
            }

            kvsBuilder.addKv(kvBuilder.build());
            bloomFilter.addString(kv.getFilename());
        }

        // write index
        final CodedOutputStream codedOutput = CodedOutputStream.newInstance(indexOutput);

        KVsProtos.KVs kvs = kvsBuilder.build();
        kvs.writeTo(codedOutput);

        BloomFilterProtos.BloomFilter.Builder bloomFilterBuilder = BloomFilterProtos.BloomFilter.newBuilder();
        BloomFilterIO.serialize(bloomFilterBuilder, bloomFilter);
        BloomFilterProtos.BloomFilter bloomFilterProtos = bloomFilterBuilder.build();
        bloomFilterProtos.writeTo(codedOutput);

        TrailerProtos.Trailer.Builder trailerBuilder = TrailerProtos.Trailer.newBuilder();
        trailerBuilder.setKvsLength(kvs.getSerializedSize());
        trailerBuilder.setMinKey(minKey);
        trailerBuilder.setMaxKey(maxKey);
        trailerBuilder.setBloomFilterLength(bloomFilterProtos.getSerializedSize());
        trailerBuilder.setVersion(SFMConstants.TRAILER_INDEX_VERSION);
        TrailerProtos.Trailer trailer = trailerBuilder.build();
        trailer.writeTo(codedOutput);

        codedOutput.flush();
        indexOutput.write(SFMUtil.getUnsignedByte(trailer.getSerializedSize()));
        LOG.debug(String.format("Generated index: [KVS %d][BloomFilter %d][Trailer %d][Last byte 1]", kvs.getSerializedSize(),
                bloomFilterProtos.getSerializedSize(), trailer.getSerializedSize()));
        indexOutput.close();

        // write master index
        masterIndexOutput.writeBytes(MasterIndexIO.serialize(new MasterIndex(indexStatus.getLen(),
                (int) (indexOutput.getPos() - indexStatus.getLen()), minKey, maxKey)));
        masterIndexOutput.close();
    }

    // small to big
    private void checkKey(String key, long nanoTime) throws IOException {
        if (lastKey != null) {
            int res = lastKey.compareTo(key);
            if (res > 0) {
                throw new IOException(String.format("The key should be ordered. Last key: %s, now key: %s", lastKey, key));
            } else if (res == 0) {
                if (lastNanoTime >= nanoTime) {
                    throw new IOException(String.format("Have the same key, but the nano time should be ordered. Last key: %s, now key: %s", lastKey, key));
                }
            }
        }

        lastKey = key;
        lastNanoTime = nanoTime;

        if (minKey == null) {
            minKey = key;
        } else {
            if (minKey.compareTo(key) > 0) {
                minKey = key;
            }
        }

        if (maxKey == null) {
            maxKey = key;
        } else {
            if (maxKey.compareTo(key) < 0) {
                maxKey = key;
            }
        }
    }
}
