package org.inlighting.sfm.merger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.index.SFMIndexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SFMerger implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SFMerger.class);

    private static final String TMP_FOLDER = SFMConstants.SFMERGER_TMP_FOLDER;

    private static final boolean OVERWRITE_FOLDER = SFMConstants.OVERWRITE_TMP_FOLDER;

    // class info

    private final long BLOCK_SIZE;

    private final Map<String, List<FileEntity>> MERGED_MAP;

    private final Map<String, Long> MERGED_MAP_SIZE;

    private final Map<String, SFMIndexBuilder> MERGED_MAP_INDEX_BUILDER;

    private boolean closed = false;

    // underlying filesystem
    private final FileSystem FS;

    public SFMerger(FileSystem fs, long blockSize) throws IOException {
        LOG.debug(String.format("Launch SFMerger, underlying FS: %s, block size %d", fs.getScheme(), blockSize));
        FS = fs;
        BLOCK_SIZE = blockSize;
        MERGED_MAP = new ConcurrentHashMap<>(5);
        MERGED_MAP_SIZE = new ConcurrentHashMap<>(5);
        MERGED_MAP_INDEX_BUILDER = new ConcurrentHashMap<>(5);

        // mkdir tmp folder
        File file = new File(TMP_FOLDER);
        if (file.exists() && !OVERWRITE_FOLDER) {
            throw new IOException("TMP_FOLDER already existed.");
        }
        FileUtils.deleteDirectory(file);
        file.mkdir();
    }

    public synchronized void add(FileEntity fileEntity) throws IOException {
        checkClosed();
        if (! fileEntity.isTombstone()) {
            checkQueueFull(fileEntity.getSfmBasePath(), fileEntity.getFilesSize());
            putInnerMergedMap(fileEntity);
        }
    }

    public synchronized void delete(FileEntity fileEntity) throws IOException {
        checkClosed();
        if (fileEntity.isTombstone()) {
            putInnerMergedMap(fileEntity);
        }
    }

    // put fileEntity in MERGED_MAP and MERGED_MAP_SIZE
    private void putInnerMergedMap(FileEntity fileEntity) {
        final String sfmBasePath = fileEntity.getSfmBasePath();
        // put in MERGED_MAP_SIZE
        if (MERGED_MAP_SIZE.containsKey(sfmBasePath)) {
            long currentSize = MERGED_MAP_SIZE.get(sfmBasePath);
            MERGED_MAP_SIZE.replace(sfmBasePath, currentSize, currentSize + fileEntity.getFilesSize());
        } else {
            MERGED_MAP_SIZE.put(sfmBasePath, fileEntity.getFilesSize());
        }
        // put in MERGED_MAP
        if (MERGED_MAP.containsKey(sfmBasePath)) {
            List<FileEntity> list = MERGED_MAP.get(sfmBasePath);
            list.add(fileEntity);
        } else {
            List<FileEntity> list = new ArrayList<>(5);
            list.add(fileEntity);
            MERGED_MAP.put(sfmBasePath, list);
        }
    }

    private void checkQueueFull(String sfmBasePath, long expectedSize) throws IOException {
        if (! MERGED_MAP_SIZE.containsKey(sfmBasePath)) {
            return;
        }

        long beforeSize = MERGED_MAP_SIZE.get(sfmBasePath);
        long futureSize = beforeSize + expectedSize;
        if (futureSize >= BLOCK_SIZE) {
            LOG.debug(String.format("(Before %d -> Future %d), need to merge %s", beforeSize, futureSize, sfmBasePath));
            merge(sfmBasePath);
        }
    }

    private void merge(String sfmBasePath) throws IOException {
        // 暂定这个名字
        final String MERGED_FILENAME = "part-0";
        Path mergeFilePath = new Path(sfmBasePath + "/" + MERGED_FILENAME);
        LOG.debug(String.format("Write index: %s, write path: %s", sfmBasePath, mergeFilePath.toUri().getPath()));

        FileStatus fileStatus;
        long startOffset;
        try {
            LOG.debug("Merged file existed, append it.");
            fileStatus = FS.getFileStatus(mergeFilePath);
            startOffset = fileStatus.getLen();
        } catch (FileNotFoundException e) {
            LOG.debug("Merged file didn't existed, create it.");
            fileStatus = null;
            startOffset = 0;
        }

        FSDataOutputStream outputStream;
        if (fileStatus != null) {
            outputStream = FS.append(mergeFilePath);
        } else {
            outputStream = FS.create(mergeFilePath);
        }

        List<FileEntity> files = MERGED_MAP.get(sfmBasePath);
        Collections.sort(files);

        // create or get index builder
        // todo
        // can improve
        SFMIndexBuilder sfmIndexBuilder;
        if (MERGED_MAP_INDEX_BUILDER.containsKey(sfmBasePath)) {
            sfmIndexBuilder = MERGED_MAP_INDEX_BUILDER.get(sfmBasePath);
            if (sfmIndexBuilder.getContainNumOfIndex() >= SFMConstants.MAX_NUM_INDEX) {
                closeIndexBuilder(sfmBasePath);
                sfmIndexBuilder = SFMIndexBuilder.build(FS, sfmBasePath, MERGED_FILENAME);
                MERGED_MAP_INDEX_BUILDER.put(sfmBasePath, sfmIndexBuilder);
            }
        } else {
            sfmIndexBuilder = SFMIndexBuilder.build(FS, sfmBasePath, MERGED_FILENAME);
            MERGED_MAP_INDEX_BUILDER.put(sfmBasePath, sfmIndexBuilder);
        }

        for (FileEntity file: files) {
            if (! file.isTombstone()) {
                // add file
                File f = new File(TMP_FOLDER + "/" + file.getTmpStoreName());
                if (! f.exists()) {
                    throw new FileNotFoundException(String.format("File %s do not existed.", file.getFilename()));
                }
                // write file to hdfs.
                byte[] tmpBytes = new byte[1024];
                FileInputStream in = new FileInputStream(f);
                int len = 0;
                while ((len = in.read(tmpBytes)) > 0) {
                    outputStream.write(tmpBytes, 0, len);
                }

                // write index to indexBuilder
                sfmIndexBuilder.add(file.getFilename(), startOffset, (int) file.getFilesSize(), file.getModificationTime());
                startOffset += outputStream.getPos();
                in.close();
                deleteFile(f);
            } else {
                // delete file
                sfmIndexBuilder.addDelete(file.getFilename(), file.getModificationTime());
            }

        }
        outputStream.close();

        MERGED_MAP.remove(sfmBasePath);
        MERGED_MAP_SIZE.remove(sfmBasePath);
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("SFMerger is closed");
        }
    }

    private void deleteFile(File file) {
        if (file.exists()) {
            file.delete();
        }
    }

    private void closeIndexBuilder(String sfmBasePath) throws IOException {
        MERGED_MAP_INDEX_BUILDER.get(sfmBasePath).close();
        MERGED_MAP_INDEX_BUILDER.remove(sfmBasePath);
    }

    @Override
    public synchronized void close() throws IOException {
        checkClosed();
        LOG.debug("SFMerger start to close.");
        try {
            // merge remain files
            LOG.debug("SFMerger start to merge remain files");
            for (String sfmBasePath: MERGED_MAP_SIZE.keySet()) {
                merge(sfmBasePath);
            }

            // generate index.
            LOG.debug("SFMerger start to generate indexes.");
            for (String sfmBasePath: MERGED_MAP_INDEX_BUILDER.keySet()) {
                closeIndexBuilder(sfmBasePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("SFMerger close failed, tmp files lost.");
        }
        LOG.debug("Delete tmp folder");
        FileUtils.deleteDirectory(new File(TMP_FOLDER));
        closed = true;
    }
}
