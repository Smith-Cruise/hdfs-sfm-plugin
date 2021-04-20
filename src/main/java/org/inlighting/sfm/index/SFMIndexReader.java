package org.inlighting.sfm.index;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.inlighting.sfm.SFMConstants;
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

    private final String MERGED_FILENAME = "part-0";

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

        loadSFMIndexStatus();

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

    public List<SFMFileStatus> listStatus() throws IOException {
        List<SFMFileStatus> fileList = new LinkedList<>();
        List<KV> kvList = new LinkedList<>();
        for (int i=0; i<masterIndexList.size(); i++) {
            IndexMetadata indexMetadata = loadIndexMetadataLazily(i);
            List<KV> tmpKvList = loadKVsLazily(indexMetadata);
            kvList.addAll(tmpKvList);
        }

        final Map<String, Boolean> EXISTED_MAP = new HashMap<>();
        ListIterator<KV> kvListIterator = kvList.listIterator(kvList.size());
        while (kvListIterator.hasPrevious()) {
            KV kv = kvListIterator.previous();
            if (! EXISTED_MAP.containsKey(kv.getFilename())) {
                EXISTED_MAP.put(kv.getFilename(), kv.isTombstone());
                if (! kv.isTombstone()) {
                    fileList.add(new SFMFileStatus(kv.getFilename(), MERGED_FILENAME, kv.getOffset(), kv.getLength(), kv.getModificationTime()));
                }
            }
        }
        return fileList;
    }

    public SFMFileStatus getFileStatus(String filename) throws IOException {
        LOG.debug(String.format("Start to read SFM FileStatus, filename: %s", filename));
        return getSFMFileStatus(filename);
    }

    public BlockLocation[] getFileBlockLocations(String filename, long start,
                                                 long len) throws IOException {
        SFMFileStatus sfmFileStatus = getSFMFileStatus(filename);
        len = Math.min(len, sfmFileStatus.getLength());
        BlockLocation[] locations = FS.getFileBlockLocations(new Path(SFM_BASE_PATH, MERGED_FILENAME),
                sfmFileStatus.getOffset() + start, len);


        // fix block
        long fileOffsetInSFM = sfmFileStatus.getOffset();
        long end = start + len;
        for (BlockLocation location: locations) {
            long sfmBlockStart = location.getOffset() - fileOffsetInSFM;
            long sfmBlockEnd = sfmBlockStart + location.getLength();

            if (start > sfmBlockStart) {
                // desired range starts after beginning of this har block
                // fix offset to beginning of relevant range (relative to desired file)
                location.setOffset(start);
                // fix length to relevant portion of har block
                location.setLength(location.getLength() - (start - sfmBlockStart));
            } else {
                // desired range includes beginning of this har block
                location.setOffset(sfmBlockStart);
            }

            if (sfmBlockEnd > end) {
                // range ends before end of this har block
                // fix length to remove irrelevant portion at the end
                location.setLength(location.getLength() - (sfmBlockEnd - end));
            }
        }
        return locations;
    }

    private SFMFileStatus getSFMFileStatus(String filename) throws IOException {
        List<Integer> probablyIndices = searchProbablyIndex(filename);
        if (probablyIndices.size() != 0) {
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
                            if (! kv.isTombstone()) {
                                return new SFMFileStatus(filename, indexMetadata.mergedFilename, kv.getOffset(), kv.getLength(), kv.getModificationTime());
                            } else {
                                throw new FileNotFoundException(String.format("%s is already deleted", filename));
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new FileNotFoundException(String.format("%s didn't exist.", filename));
    }

    private synchronized IndexMetadata loadIndexMetadataLazily(int indexId) throws IOException {
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

    private synchronized BloomFilter loadBloomFilterLazily(IndexMetadata indexMetadata) throws IOException {
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

    private synchronized List<KV> loadKVsLazily(IndexMetadata indexMetadata) throws IOException {
        if (KV_LIST_CACHE.containsKey(indexMetadata.indexId)) {
            return KV_LIST_CACHE.get(indexMetadata.indexId);
        } else {
            IN.seek(indexMetadata.offset + indexMetadata.length - 1
                    - indexMetadata.trailerLength - indexMetadata.bloomFilterLength - indexMetadata.kvsLength);
            byte[] kvsBytes = new byte[indexMetadata.kvsLength];
            IN.readFully(kvsBytes);
            KVsProtos.KVs kvsProtos = KVsProtos.KVs.parseFrom(kvsBytes);

            final int kvSize = kvsProtos.getKvCount();
            List<KV> kvList = new ArrayList<>();
            List<KVsProtos.KV> kvProtosList = kvsProtos.getKvList();
            for (int i=0; i<kvSize; i++) {
                KVsProtos.KV kvProtos = kvProtosList.get(i);
                if (! kvProtos.getTombstone()) {
                    // create
                    kvList.add(new KV(kvProtos.getFilename(), kvProtos.getOffset(), kvProtos.getLength(),
                            kvProtos.getModificationTime(), kvProtos.getTombstone()));
                } else {
                    // delete
                    kvList.add(new KV(kvProtos.getFilename(), 0, 0,
                            kvProtos.getModificationTime(), kvProtos.getTombstone()));
                }
            }
            KV_LIST_CACHE.put(indexMetadata.indexId, kvList);
            return kvList;
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

    private synchronized void loadSFMIndexStatus() throws IOException {
        // check master index.
        FileStatus masterIndexStat = null;
        try {
            masterIndexStat = FS.getFileStatus(MASTER_INDEX_PATH);
        } catch (FileNotFoundException e) {
            throw new IOException(String.format("Cannot find %s for sfm.", SFMConstants.MASTER_INDEX_NAME));
        }
        if (! masterIndexStat.isFile()) {
            throw new IOException("Master index is not a regular file.");
        }

        // check index
        FileStatus indexStat = null;
        try {
            indexStat = FS.getFileStatus(INDEX_PATH);
        } catch (FileNotFoundException e) {
            throw new IOException(String.format("Cannot find %s for sfm.", SFMConstants.INDEX_NAME));
        }
        // check index is a dictionary
        if (! indexStat.isFile()) {
            throw new IOException("Index is not a regular file.");
        }
        LOG.debug("SFM index check ok");

        // start to load master index
        LOG.debug("Start to load master index.");
        BLOCK_SIZE = masterIndexStat.getBlockSize();
        REPLICATION = masterIndexStat.getReplication();

        FSDataInputStream in = FS.open(MASTER_INDEX_PATH);
        long read = 0;
        Text line = new Text();
        LineReader lineReader = new LineReader(in);
        masterIndexList = new ArrayList<>();
        while (read < masterIndexStat.getLen()) {
            int b = lineReader.readLine(line);
            read += b;
            String[] tmpStr = line.toString().split(",");
            masterIndexList.add(new MasterIndex(Long.parseLong(tmpStr[0]), Integer.parseInt(tmpStr[1]),
                    tmpStr[2], tmpStr[3]));
        }
        lineReader.close();
        LOG.debug("SFM master index load succeed.");
    }

    private String getKey(List<KV> kvs, int index) {
        return kvs.get(index).getFilename();
    }

    // if match the same key, return last one.
    private KV binarySearch(List<KV> kvs, String key) {
        int left = 0;
        int right = kvs.size() - 1;
        int mid;

        if (key.compareTo(getKey(kvs, left)) < 0 || key.compareTo(getKey(kvs, right)) > 0) {
            return null;
        }

        while (left <= right) {
            int compare = 0;
            mid = (left + right) / 2;
            compare = getKey(kvs, mid).compareTo(key);
            if (compare == 0) {
                left = mid + 1;
            } else if (compare < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if (right != -1 && getKey(kvs, right).equals(key)) {
            return kvs.get(right);
        }
        return null;
    }

    public long getBlockSize() {
        return BLOCK_SIZE;
    }

    public short getReplication() {
        return REPLICATION;
    }

    public String getSFMBasePath() {
        return SFM_BASE_PATH;
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
}
