package org.inlighting.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hadoop.util.Progressable;
import org.inlighting.SFMConstants;
import org.inlighting.index.SFMIndexReader;
import org.inlighting.merger.FileEntity;
import org.inlighting.merger.SFMerger;
import org.inlighting.merger.SFMergerFactory;
import org.inlighting.util.LruCache;
import org.inlighting.util.SFMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

public class SFMFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(SFMFileSystem.class);

    private Map<String, SFMetaData> metaDataCache;

    private final int MAX_CACHE_ENTRIES = 10;

    private SFMerger SFMerger;

    private Path workingDir;

    private boolean closed = false;

    // current uri
    private URI uri;

    // specific to merged file system.
    private final String DEFAULT_UNDERLYING_FS = SFMConstants.DEFAULT_UNDERLYING_FS;
    private final long DEFAULT_BLOCK_SIZE = SFMConstants.DEFAULT_BLOCK_SIZE;
    private FileSystem underLyingFS;

    // current information, need lock
    private String curSFMBasePath;
    private SFMIndexReader curSFMReader;

    public SFMFileSystem() {
        // need to initialize
    }

    public SFMFileSystem(FileSystem fs) {
        underLyingFS = fs;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);

        checkValidSFMURI(name);
        uri = name;

        // create underlying file system
        if (underLyingFS == null) {
            // create Distributed FileSystem
            underLyingFS = new Path(String.format("%s://%s", DEFAULT_UNDERLYING_FS, name.getAuthority())).getFileSystem(conf);
        }
        if (metaDataCache == null) {
            metaDataCache = Collections.synchronizedMap(new LruCache<String, SFMetaData>(MAX_CACHE_ENTRIES));
        }
    }

    private void loadSFMInformation(URI name) throws IOException {
        String sfmBasePath = SFMUtil.getSFMBasePath(name);
        if (! sfmBasePath.equals(curSFMBasePath)) {
            SFMetaData metaData = getSFMInformation(sfmBasePath);
            curSFMBasePath = metaData.sfmBasePath;
            curSFMReader = metaData.sfmReader;
        } else {
            LOG.debug(String.format("%s do not need to reload SFM information.", uri.toString()));
        }
    }

    private SFMetaData getSFMInformation(String sfmBasePath) throws IOException {
        LOG.debug(String.format("Load SFM information: %s", sfmBasePath));
        SFMetaData metaData = metaDataCache.get(sfmBasePath);
        if (metaData == null) {
            // build SFMIndexReader
            SFMIndexReader sfmReader = SFMIndexReader.build(underLyingFS, sfmBasePath);
            SFMetaData newSFMetaData = new SFMetaData(sfmBasePath, sfmReader);
            metaDataCache.put(sfmBasePath, newSFMetaData);
            return newSFMetaData;
        } else {
            return metaData;
        }
    }

    // check valid sfm url. If schema == null, make sure uri is a absolute path.
    private void checkValidSFMURI(URI rawURI) throws IOException {
        SFMUtil.checkValidSFM(rawURI);
        String schema = rawURI.getScheme();
        String authority = rawURI.getAuthority();

        if (schema != null) {
            if (!schema.equals(getScheme()) || authority == null) {
                throw new IOException("URI schema should be sfm:// .");
            }
        }
    }

    private void checkValidSFMURI(Path path) throws IOException {
        checkValidSFMURI(path.toUri());
    }

    @Override
    public Configuration getConf() {
        return underLyingFS.getConf();
    }

    @Override
    public String getScheme() {
        return "sfm";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        checkValidSFMURI(path);
        loadSFMInformation(path.toUri());
        // start to read
        String filename = SFMUtil.getFilename(path.toUri());
        return curSFMReader.read(filename, bufferSize);
    }

    // todo
    // 永远默认覆盖文件
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        checkValidSFMURI(path);

        if (! overwrite) {
            LOG.info("SFM filesystem always overwrite data.");
        }

        // Load SFMerger lazily.(Only load it when need to create file.)
        if (SFMerger == null) {
            SFMerger = SFMergerFactory.build(underLyingFS, DEFAULT_BLOCK_SIZE);
        }

        String sfmBasePath = SFMUtil.getSFMBasePath(path.toUri());
        String filename = SFMUtil.getFilename(path.toUri());
        LOG.debug(String.format("Upload file SFM base path: %s, Filename: %s", sfmBasePath, filename));
        // ignore all parameters except Path
        return new SFMFsDataOutputStream(SFMerger, sfmBasePath, filename, statistics);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        checkValidSFMURI(f);
        throw new IOException("Append is not supported!");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkValidSFMURI(src);
        throw new IOException("Rename is not supported!");
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        // todo
        // Support inner mkdir.
        checkValidSFMURI(path);
        if (recursive) {
            throw new IOException("Recursive delete is not supported!");
        }

        // Load SFMerger lazily.(Only load it when need to create file.)
        if (SFMerger == null) {
            SFMerger = SFMergerFactory.build(underLyingFS, DEFAULT_BLOCK_SIZE);
        }
        String sfmBasePath = SFMUtil.getSFMBasePath(path.toUri());
        String filename = SFMUtil.getFilename(path.toUri());
        LOG.debug(String.format("Delete file SFM base path: %s, Filename: %s", sfmBasePath, filename));
        SFMerger.delete(new FileEntity(sfmBasePath, filename, null, 0, true));
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        checkValidSFMURI(path);
        // path should end with .sfm
        if (! path.toUri().getPath().endsWith(".sfm")) {
            throw new IOException("Path should end with .sfm");
        }
        loadSFMInformation(path.toUri());
        return curSFMReader.listFiles();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        workingDir = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return underLyingFS.getStatus(p);
    }

    @Override
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new IOException("Mkdir is not supported!");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        checkValidSFMURI(path);
        loadSFMInformation(path.toUri());

        // todo
        return new FileStatus(0, true, 1, 128*1024*1024, 0, path);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            throw new IOException("SFMFileSystem is closed.");
        }
        LOG.debug("Start to close SFMFileSystem.");
        if (SFMerger != null) {
            SFMerger.close();
            LOG.debug("SFMerger closed.");
        }
        underLyingFS.close();
        LOG.debug("Underlying FileSystem closed.");
        closed = true;
    }

    private class SFMetaData {

        private String sfmBasePath;

        private SFMIndexReader sfmReader;

        public SFMetaData(String sfmBasePath, SFMIndexReader sfmReader) {
            this.sfmBasePath = sfmBasePath;
            this.sfmReader = sfmReader;
        }
    }

}
