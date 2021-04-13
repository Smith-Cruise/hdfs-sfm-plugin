package org.inlighting.sfm.index;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.fs.SFMFsDataInputStream;
import org.inlighting.sfm.proto.BloomFilterProtos;
import org.inlighting.sfm.proto.KVsProtos;
import org.inlighting.sfm.proto.TrailerProtos;
import org.inlighting.sfm.util.BloomFilter;
import org.inlighting.sfm.util.BloomFilterIO;
import org.inlighting.sfm.util.LruCache;
import org.inlighting.sfm.util.SFMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class SFMIndexReader {

    private static final Logger LOG = LoggerFactory.getLogger(SFMIndexReader.class);

    // class info
    private final FileSystem FS;

    private final String SFM_BASE_PATH;

    private final Path INDEX_PATH;

    private final Path MASTER_INDEX_PATH;

    private final FSDataInputStream IN;

    // held master index info
    // data should ordered from new to old.
    private List<MasterIndex> masterIndexList;
    private long BLOCK_SIZE;
    private short REPLICATION;

    // held cache for index & bloom filter
    private final int MAX_INDEX_METADATA = 50;
    private final LruCache<Integer, IndexMetadata> INDEX_METADATA_CACHE;
    private final int MAX_BLOOM_FILTER_CACHE = 50;
    private final LruCache<Integer, BloomFilter> BLOOM_FILTER_CACHE;
    private final int MAX_INDEX_CACHE = 5;
    private final LruCache<Integer, List<KV>> KV_LIST_CACHE;

    public SFMIndexReader(FileSystem fs, String sfmBasePath) throws IOException {
        FS = fs;
        SFM_BASE_PATH = sfmBasePath;
        MASTER_INDEX_PATH = new Path(SFM_BASE_PATH + "/" + SFMConstants.MASTER_INDEX_NAME);
        INDEX_PATH = new Path(SFM_BASE_PATH + "/" + SFMConstants.INDEX_NAME);

        checkSFMStatus();

        // load master index
        loadMasterIndex();

        INDEX_METADATA_CACHE = new LruCache<>(MAX_INDEX_METADATA);
        // initialize bloom filter cache
        BLOOM_FILTER_CACHE = new LruCache<>(MAX_BLOOM_FILTER_CACHE);
        // initialize index metadata cache
        KV_LIST_CACHE = new LruCache<>(MAX_INDEX_CACHE);

        // open index path
        IN = fs.open(INDEX_PATH);
    }

    // must to check index validity before build SFMIndexReader.
    public static SFMIndexReader build(FileSystem fs, String sfmBasePath) throws IOException {
        return new SFMIndexReader(fs, sfmBasePath);
    }

    public FileStatus[] listStatus() throws IOException {
        List<KV> kvList = new LinkedList<>();
        for (int i=0; i<masterIndexList.size(); i++) {
            IndexMetadata indexMetadata = loadIndexMetadataLazily(i);
            List<KV> tmpKvList = loadKVsLazily(indexMetadata);
            kvList.addAll(tmpKvList);
        }

        final Map<String, Boolean> EXISTED_MAP = new HashMap<>();
        Iterator<KV> kvIterator = kvList.listIterator();
        while (kvIterator.hasNext()) {
            KV kv = kvIterator.next();

            if (EXISTED_MAP.containsKey(kv.filename)) {
                kvIterator.remove();
            } else {
                EXISTED_MAP.put(kv.filename, kv.tombstone);
                if (kv.tombstone) {
                    kvIterator.remove();
                }
            }
        }

        FileStatus[] fileStatus = new FileStatus[kvList.size()];
        final Path sfmBasePath = new Path(SFM_BASE_PATH);
        int i = 0;
        for (KV kv: kvList) {
            fileStatus[i] = new FileStatus(kv.length, false, REPLICATION, BLOCK_SIZE, 0, new Path(sfmBasePath, kv.filename));
            i++;
        }
        return fileStatus;
    }

    public FileStatus getFileStatus(String filename) throws IOException {
        SFMFileStatus sfmFileStatus = getSFMFileStatus(filename);
        return new FileStatus(sfmFileStatus.length, false, REPLICATION,
                BLOCK_SIZE, 0, new Path(SFM_BASE_PATH + "/" + filename));
    }

    private SFMFileStatus getSFMFileStatus(String filename) throws IOException {
        List<Integer> probablyIndices = searchProbablyIndex(filename);
        if (probablyIndices.size() == 0) {
            throw new FileNotFoundException(String.format("%s didn't existed.", filename));
        }

        try {
            for (int probablyIndex: probablyIndices) {
                // get index metadata
                IndexMetadata indexMetadata = loadIndexMetadataLazily(probablyIndex);

                // check is in bloom filter
                BloomFilter bloomFilter = loadBloomFilterLazily(indexMetadata);

                boolean found = bloomFilter.testString(filename);
                if (found) {
                    List<KV> kvs = loadKVsLazily(indexMetadata);

                    // binary search filename
                    KV kv = binarySearch(kvs, filename);
                    if (kv != null) {
                        if (! kv.tombstone) {
                            return new SFMFileStatus(filename, indexMetadata.mergedFilename, kv.offset, kv.length);

                        } else {
                            throw new FileNotFoundException(String.format("%s is already delete", filename));
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        throw new FileNotFoundException(String.format("%s didn't exist.", filename));
    }

    public FSDataInputStream read(String filename, int bufferSize) throws IOException {
        SFMFileStatus sfmFileStatus = getSFMFileStatus(filename);
        return new SFMFsDataInputStream(FS, new Path(SFM_BASE_PATH + "/" +sfmFileStatus.mergedFilename),
                sfmFileStatus.offset, sfmFileStatus.length, bufferSize);
    }

    private IndexMetadata loadIndexMetadataLazily(int indexId) throws IOException {
        if (INDEX_METADATA_CACHE.containsKey(indexId)) {
            return INDEX_METADATA_CACHE.get(indexId);
        } else {
            // start to read index from hdfs.
            MasterIndex masterIndex = masterIndexList.get(indexId);

            long offset = masterIndex.getOffset();
            int length = masterIndex.getLength();

            IndexMetadata indexMetadata = new IndexMetadata();

            IN.seek(offset + length - 1);
            // read last byte
            int trailerLength = SFMUtil.readUnsignedByte(IN.readByte());

            // read trailer
            IN.seek((offset + length - 1 - trailerLength));
            byte[] trailerBytes = new byte[trailerLength];
            IN.readFully(trailerBytes);
            TrailerProtos.Trailer trailer = TrailerProtos.Trailer.parseFrom(trailerBytes);
            String mergedFilename = trailer.getMergedFilename();
//                String minKey = trailer.getMinKey();
//                String maxKey = trailer.getMaxKey();
            int version = trailer.getVersion();
            int bloomFilterLength = trailer.getBloomFilterLength();
            int kvsLength = trailer.getKvsLength();

            indexMetadata.indexId = indexId;
            indexMetadata.offset = offset;
            indexMetadata.length = length;
            indexMetadata.mergedFilename = mergedFilename;
//                indexMetadata.minKey = minKey;
//                indexMetadata.maxKey = maxKey;
            indexMetadata.version = version;
            indexMetadata.bloomFilterLength = bloomFilterLength;
            indexMetadata.kvsLength = kvsLength;
            indexMetadata.trailerLength = trailerLength;
            return indexMetadata;
        }
    }

    private BloomFilter loadBloomFilterLazily(IndexMetadata indexMetadata) throws IOException {
        if (BLOOM_FILTER_CACHE.containsKey(indexMetadata.indexId)) {
            return BLOOM_FILTER_CACHE.get(indexMetadata.indexId);
        } else {
            IN.seek(indexMetadata.offset + indexMetadata.length - 1 - indexMetadata.trailerLength - indexMetadata.bloomFilterLength);
            byte[] bloomFilterBytes = new byte[indexMetadata.bloomFilterLength];
            IN.readFully(bloomFilterBytes);
            BloomFilterProtos.BloomFilter bloomFilterProtos = BloomFilterProtos.BloomFilter.parseFrom(bloomFilterBytes);
            BloomFilter bloomFilter = BloomFilterIO.deserialize(bloomFilterProtos);
            BLOOM_FILTER_CACHE.put(indexMetadata.indexId, bloomFilter);
            return bloomFilter;
        }
    }

    private List<KV> loadKVsLazily(IndexMetadata indexMetadata) throws IOException {
        if (KV_LIST_CACHE.containsKey(indexMetadata.indexId)) {
            return KV_LIST_CACHE.get(indexMetadata.indexId);
        } else {
            IN.seek(indexMetadata.offset + indexMetadata.length - 1
                    - indexMetadata.trailerLength - indexMetadata.bloomFilterLength - indexMetadata.kvsLength);
            byte[] kvsBytes = new byte[indexMetadata.kvsLength];
            IN.readFully(kvsBytes);
            KVsProtos.KVs kvsProtos = KVsProtos.KVs.parseFrom(kvsBytes);

            final int kvSize = kvsProtos.getKvCount();
            KV[] kvs = new KV[kvSize];
            List<KVsProtos.KV> kvProtosList = kvsProtos.getKvList();
            for (int i=0; i<kvSize; i++) {
                KVsProtos.KV kvProtos = kvProtosList.get(i);
                if (! kvProtos.getTombstone()) {
                    // create
                    kvs[kvSize-i-1] = new KV(kvProtos.getFilename(), kvProtos.getOffset(), kvProtos.getLength(), kvProtos.getTombstone());
                } else {
                    // delete
                    kvs[kvSize-i-1] = new KV(kvProtos.getFilename(), 0, 0, kvProtos.getTombstone());
                }
            }
            List<KV> kvsList = Arrays.asList(kvs);
            KV_LIST_CACHE.put(indexMetadata.indexId, kvsList);
            return kvsList;
        }
    }

    private List<Integer> searchProbablyIndex(String filename) {
        List<Integer> list = new ArrayList<>();
        for (int i=0; i<masterIndexList.size(); i++) {
            MasterIndex index = masterIndexList.get(i);
            if (filename.compareTo(index.getMinKey()) >= 0 && filename.compareTo(index.getMaxKey()) <= 0) {
                list.add(i);
            }
        }
        return list;
    }

    private void loadMasterIndex() throws IOException {
        LOG.debug("Load master index.");
        FileStatus masterIndexStatus = FS.getFileStatus(MASTER_INDEX_PATH);
        BLOCK_SIZE = masterIndexStatus.getBlockSize();
        REPLICATION = masterIndexStatus.getReplication();

        FSDataInputStream in = FS.open(MASTER_INDEX_PATH);
        long read = 0;
        Text line = new Text();
        LineReader lineReader = new LineReader(in);
        masterIndexList = new ArrayList<>();
        while (read < masterIndexStatus.getLen()) {
            int b = lineReader.readLine(line);
            read += b;
            String[] tmpStr = line.toString().split(",");
            masterIndexList.add(0, new MasterIndex(Long.parseLong(tmpStr[0]), Integer.parseInt(tmpStr[1]),
                    tmpStr[2], tmpStr[3]));
        }
    }

    private void checkSFMStatus() throws IOException {
        // check master index existed.
        if (! FS.exists(MASTER_INDEX_PATH)) {
            throw new IOException(String.format("Cannot find %s for sfm.", SFMConstants.MASTER_INDEX_NAME));
        }

        // check index existed.
        if (! FS.exists(INDEX_PATH)) {
            throw new IOException(String.format("Cannot find %s for sfm.", SFMConstants.INDEX_NAME));
        }

        // check master index is a dictionary
        FileStatus masterIndexStat = FS.getFileStatus(MASTER_INDEX_PATH);
        if (! masterIndexStat.isFile()) {
            throw new IOException("Master index is not a regular file.");
        }

        // check index is a dictionary
        FileStatus indexStat = FS.getFileStatus(INDEX_PATH);
        if (! indexStat.isFile()) {
            throw new IOException("Index is not a regular file.");
        }
    }

    private String getKey(List<KV> kvs, int index) {
        return kvs.get(index).filename;
    }

    private KV binarySearch(List<KV> kvs, String key) {
        int low = 0;
        int high = kvs.size() - 1;
        int middle = 0;
        if (key.compareTo(getKey(kvs, low)) < 0 || key.compareTo(getKey(kvs, high)) > 0) {
            return null;
        }

        while (low <= high) {
            middle = (low + high) / 2;
            if (getKey(kvs, middle).equals(key)) {
                return kvs.get(middle);
            } else if (getKey(kvs, middle).compareTo(key) < 0) {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        return null;
    }

    private class IndexMetadata {

        private int indexId;

        private long offset;

        private int length;

        private String mergedFilename;

//        private String minKey;

//        private String maxKey;

        private int version;

        private int trailerLength;

        private int bloomFilterLength;

        private int kvsLength;
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

    private class SFMFileStatus {
        private String filename;

        private String mergedFilename;

        private long offset;

        private int length;

        public SFMFileStatus(String filename, String mergedFilename, long offset, int length) {
            this.filename = filename;
            this.mergedFilename = mergedFilename;
            this.offset = offset;
            this.length = length;
        }
    }
}
