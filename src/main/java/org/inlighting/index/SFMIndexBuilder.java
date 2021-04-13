package org.inlighting.index;

import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.SFMConstants;
import org.inlighting.proto.BloomFilterProtos;
import org.inlighting.proto.KVsProtos;
import org.inlighting.proto.TrailerProtos;
import org.inlighting.util.BloomFilter;
import org.inlighting.util.BloomFilterIO;
import org.inlighting.util.SFMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SFMIndexBuilder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SFMIndexBuilder.class);

    private final FileSystem FS;

    private final String SFM_BASE_PATH;

    private final String MERGE_FILENAME;

    private int numIndex = 0;

    // bloom filter
    private static final double FPP = 0.05;

    // trailer info

    private String minKey;

    private String maxKey;

    private String lastKey;

    // KV
    private final List<KV> INDEX_LIST;

    public SFMIndexBuilder(FileSystem fs, String sfmBasePath, String mergeFilename) {
        FS = fs;
        SFM_BASE_PATH = sfmBasePath;
        MERGE_FILENAME = mergeFilename;
        INDEX_LIST = new LinkedList<>();
    }

    public static SFMIndexBuilder build(FileSystem fs, String sfmBasePath, String mergeFilename) {
        return new SFMIndexBuilder(fs, sfmBasePath, mergeFilename);
    }

    public void add(String filename, long offset, int length) throws IOException {
        checkKey(filename);

        INDEX_LIST.add(new KV(filename, offset, length, false));
        numIndex++;
    }

    public void addDelete(String filename) throws IOException {
        checkKey(filename);
        INDEX_LIST.add(new KV(filename, 0, 0, true));
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

        INDEX_LIST.forEach(kv -> {
            KVsProtos.KV.Builder kvBuilder = KVsProtos.KV.newBuilder();
            if (! kv.tombstone) {
                kvBuilder.setFilename(kv.filename);
                kvBuilder.setOffset(kv.offset);
                kvBuilder.setLength(kv.length);
                kvBuilder.setTombstone(false);
            } else {
                kvBuilder.setFilename(kv.filename);
                kvBuilder.setTombstone(true);
            }

            kvsBuilder.addKv(kvBuilder.build());
            bloomFilter.addString(kv.filename);
        });

        // write index
        final CodedOutputStream codedOutput = CodedOutputStream.newInstance(indexOutput);

        KVsProtos.KVs kvs = kvsBuilder.build();
        kvs.writeTo(codedOutput);

        BloomFilterProtos.BloomFilter.Builder bloomFilterBuilder = BloomFilterProtos.BloomFilter.newBuilder();
        BloomFilterIO.serialize(bloomFilterBuilder, bloomFilter);
        BloomFilterProtos.BloomFilter bloomFilterProtos = bloomFilterBuilder.build();
        bloomFilterProtos.writeTo(codedOutput);

        TrailerProtos.Trailer.Builder trailerBuilder = TrailerProtos.Trailer.newBuilder();
        trailerBuilder.setMergedFilename(MERGE_FILENAME);
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
    private void checkKey(String key) throws IOException {
        if (lastKey == null) {
            lastKey = key;
        }

        if (lastKey.compareTo(key) > 0) {
            throw new IOException("The key should be ordered.");
        }
        lastKey = key;

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

    private class KV {
        private String filename;

        private long offset;

        private int length;

        private boolean tombstone;

        public KV(String filename, long offset, int length, boolean tombstone) {
            this.filename = filename;
            this.offset = offset;
            this.length = length;
            this.tombstone = tombstone;
        }
    }
}
