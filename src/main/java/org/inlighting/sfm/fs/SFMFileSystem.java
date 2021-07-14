package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;
import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.index.SFMFileStatus;
import org.inlighting.sfm.index.SFMIndexReader;
import org.inlighting.sfm.merger.FileEntity;
import org.inlighting.sfm.merger.SFMerger;
import org.inlighting.sfm.merger.SFMergerFactory;
import org.inlighting.sfm.readahead.ReadaheadManager;
import org.inlighting.sfm.readahead.SPSAReadaheadManager;
import org.inlighting.sfm.readahead.StaticReadaheadManager;
import org.inlighting.sfm.util.LruCache;
import org.inlighting.sfm.util.SFMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class SFMFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(SFMFileSystem.class);

    private Map<String, SFMetaData> metaDataCache;

    private SFMerger SFMerger;

    // include schema & authority, sfm://xxxx.com:9000, identity sfm file system
    private URI uri;
    // /hello-world, used for relative path, without schema and authority
    private Path workingDir;

    private boolean closed = false;

    private FileSystem underLyingFS;

    // current information, need lock
    private String curSFMBasePath;
    private SFMIndexReader curSFMReader;
    private ReadaheadManager curReadaheadManager;

    public SFMFileSystem() {
        // need to initialize
    }

    public SFMFileSystem(FileSystem fs) {
        underLyingFS = fs;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        LOG.debug("Initialize SFM FileSystem.");
        super.initialize(name, conf);
        setConf(conf);

        // init file system, name must contains all information(schema, authority...)
        String schema = name.getScheme();
        if (!schema.equalsIgnoreCase(getScheme())) {
            throw new IOException("URI schema should be sfm");
        }
        String host = name.getHost();
        if (host == null) {
            throw new IOException("Incomplete SFM URI, no host: "+name);
        }
        String authority = name.getAuthority();
        if (authority == null) {
            throw new IOException("Incomplete SFM URI, no authority: "+name);
        }

        synchronized (this) {
            // create underlying file system
            if (underLyingFS == null) {
                // create Distributed FileSystem without cache !!!!
                // specific to merged file system.
                conf.set("fs.hdfs.impl.disable.cache", "true");
                underLyingFS = new Path(String.format("%s://%s", SFMConstants.DEFAULT_UNDERLYING_FS, authority)).getFileSystem(conf);
                LOG.debug(String.format("Uncached underlying FS create succeed. Schema: %s, Details: %s", underLyingFS.getScheme(),
                        underLyingFS.toString()));
            }

            this.uri = URI.create(schema+"://"+authority);
            this.workingDir = getHomeDirectory();

            // init metaDataCache
            if (metaDataCache == null) {
                final int MAX_CACHE_ENTRIES = 10;
                metaDataCache = Collections.synchronizedMap(new LruCache<String, SFMetaData>(MAX_CACHE_ENTRIES));
            }
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Path absF = fixRelativePart(new_dir);
        String result = absF.toUri().getPath();
        if (!DFSUtilClient.isValidName(result)) {
            throw new IllegalArgumentException("Invalid DFS directory name " +
                    result);
        }
        workingDir = absF;
    }

    @Override
    public Path getHomeDirectory() {
        return underLyingFS.getHomeDirectory();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
                                                 long len) throws IOException {
        if (SFMConstants.ENABLE_CACHE) {
            // todo
            return super.getFileBlockLocations(file, start, len);
        }
        LOG.debug(String.format("GetFileBlockLocations, file path: %s, start: %d, len:%d", file.getPath(),
                start, len));
        if (start < 0 || len < 0) {
            throw new IllegalArgumentException("Invalid start or len parameter");
        }

        Path absF = fixRelativePart(file.getPath());
        checkPath(absF);
        loadSFMInformation(absF.toUri());
        String filename = SFMUtil.getFilename(absF.toUri());
        if (filename != null) {
           return curSFMReader.getFileBlockLocations(filename, start, len);
        } else {
            return underLyingFS.getFileBlockLocations(new Path("hdfs", absF.toUri().getAuthority(), absF.toUri().getPath()),
                    start, len);
        }
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        Path absF = fixRelativePart(f);
        checkPath(absF);
        loadSFMInformation(absF.toUri());
        // start to read
        String filename = SFMUtil.getFilename(absF.toUri());
        if (filename == null) {
            throw new FileNotFoundException(absF.toString());
        }
        SFMFileStatus sfmFileStatus = curSFMReader.getFileStatus(filename);
        return new SFMFsDataInputStream(underLyingFS, new Path(curSFMBasePath, SFMConstants.MERGED_FILENAME),
                sfmFileStatus.getOffset(), sfmFileStatus.getLength(), bufferSize, curReadaheadManager);
    }

    @Override
    public FSDataOutputStream append(Path f, final int bufferSize,
                                     final Progressable progress) throws IOException {
        throw new IOException("Append unsupported");
    }

    // SFM filesystem always overwrite data.
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        Path absF = fixRelativePart(f);
        checkPath(absF);
        URI uri = absF.toUri();

        if (! overwrite) {
            LOG.info("SFM filesystem always overwrite data.");
        }

        // Load SFMerger lazily.(Only load it when need to create file.)
        if (SFMerger == null) {
            SFMerger = SFMergerFactory.build(underLyingFS, blockSize);
        }

        String sfmBasePath = SFMUtil.getSFMBasePath(uri);
        String filename = SFMUtil.getFilename(uri);
        LOG.debug(String.format("Upload file SFM base path: %s, Filename: %s", sfmBasePath, filename));
        // ignore all parameters except Path
        return new SFMFsDataOutputStream(SFMerger, sfmBasePath, filename, statistics);
    }

    // setReplication can only set for the whole sfm dir.
    // not test yet
    // todo
    @Override
    public boolean setReplication(Path src, final short replication)
            throws IOException {
        Path absF = fixRelativePart(src);
        String sfmBasePath = SFMUtil.getSFMBasePath(absF.toUri());
        return underLyingFS.setReplication(new Path(sfmBasePath), replication);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new IOException("Rename unsupported");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        // todo
        // Support inner mkdir in the future

        Path absF = fixRelativePart(f);
        checkPath(absF);
        URI uri = absF.toUri();

        if (recursive) {
            throw new IOException("Recursive delete is not supported!");
        }

        // Load SFMerger lazily.(Only load it when need to create file.)
        if (SFMerger == null) {
            SFMerger = SFMergerFactory.build(underLyingFS,
                    getConf().getLong("fs.local.block.size", 32 * 1024 * 1024));
        }
        String sfmBasePath = SFMUtil.getSFMBasePath(uri);
        String filename = SFMUtil.getFilename(uri);
        LOG.debug(String.format("Delete file SFM base path: %s, Filename: %s", sfmBasePath, filename));
        SFMerger.delete(new FileEntity(sfmBasePath, filename, null, 0, System.currentTimeMillis(), true, System.nanoTime()));
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        Path absF = fixRelativePart(f);
        checkPath(absF);
        URI uri = absF.toUri();
        // path should be a directory, end with .sfm
        if (! uri.getPath().endsWith(".sfm")) {
            throw new IOException("Path should end with .sfm");
        }
        loadSFMInformation(uri);

        List<SFMFileStatus> sfmFileStatuses= curSFMReader.listStatus();

        // sort it to boost file affinity
//        Collections.sort(sfmFileStatuses);

        FileStatus[] fileStatuses = new FileStatus[sfmFileStatuses.size()];
        int i = 0;
        for (SFMFileStatus sfmFileStatus: sfmFileStatuses) {
            final Path filePath = makeQualified(new Path(curSFMBasePath, sfmFileStatus.getFilename()));
            fileStatuses[i] = new FileStatus(sfmFileStatus.getLength(), false, curSFMReader.getReplication(),
                    curSFMReader.getBlockSize(), sfmFileStatus.getModificationTime(), filePath);
            i++;
        }
        return fileStatuses;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new IOException("Unsupported");
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
        LOG.debug("Start to close all SFMetaData");
        for (String sfmBasePath: metaDataCache.keySet()) {
            SFMetaData sfMetaData = metaDataCache.get(sfmBasePath);
            sfMetaData.sfmReader.close();
            if (SFMConstants.ENABLE_CACHE) {
                sfMetaData.readaheadManager = null;
            }
        }

        underLyingFS.close();
        LOG.debug("Underlying FileSystem closed.");
        closed = true;
        super.close();
    }

    @Override
    public String toString() {
        return "SFM[" + uri + "]";
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return underLyingFS.getStatus(p);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Path absF = fixRelativePart(f);
        checkPath(absF);
        URI uri = absF.toUri();
        loadSFMInformation(uri);

        String filename = SFMUtil.getFilename(uri);
        if (filename != null) {
            // file
            SFMFileStatus sfmFileStatus = curSFMReader.getFileStatus(filename);
            final Path filePath = makeQualified(new Path(curSFMBasePath, sfmFileStatus.getFilename()));
            return new FileStatus(sfmFileStatus.getLength(), false, curSFMReader.getReplication(),
                    curSFMReader.getBlockSize(), sfmFileStatus.getModificationTime(), filePath);
        } else {
            // dir, handle by underlyingFS
            // if absF contains sfm schema, should remove it.
            FileStatus fileStatus = underLyingFS.getFileStatus(new Path(absF.toUri().getPath()));
            // replace schema from hdfs to sfm
            URI tmpURI = fileStatus.getPath().toUri();
            fileStatus.setPath(new Path(getScheme(), tmpURI.getAuthority(), tmpURI.getPath()));
            return fileStatus;
        }
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return underLyingFS.getDefaultBlockSize(f);
    }

    @Override
    public short getDefaultReplication(Path path) {
        return underLyingFS.getDefaultReplication(path);
    }

    @Override
    public void setPermission(Path p, final FsPermission permission
    ) throws IOException {
        throw new IOException("Unsupported");
    }

    @Override
    public void setOwner(Path p, final String username, final String groupname)
            throws IOException {
        throw new IOException("Unsupported");
    }

    @Override
    public void setTimes(Path p, final long mtime, final long atime)
            throws IOException {
        throw new IOException("Unsupported");
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        if (HAUtilClient.isLogicalUri(getConf(), uri)) {
            // Don't try to DNS-resolve logical URIs, since the 'authority'
            // portion isn't a proper hostname
            return uri;
        } else {
            return NetUtils.getCanonicalUri(uri, getDefaultPort());
        }
    }

    @Override
    public FSDataOutputStreamBuilder createFile(Path path) {
        return underLyingFS.createFile(path);
    }

    @Override
    public FSDataOutputStreamBuilder appendFile(Path path) {
        return underLyingFS.appendFile(path);
    }

    @Override
    public boolean hasPathCapability(final Path path, final String capability)
            throws IOException {
        // todo
        throw new IOException("Unsupported");
    }

    // path must be absolute
    // getTrashRoot
    // makeQualified
    // checkPath
    // todo
    @Override
    protected void checkPath(Path path) {
        if (path.isAbsolute()) {
            URI uri = path.toUri();

            String thatSchema = uri.getScheme();
            String thatAuthority = uri.getAuthority();

            if (thatSchema != null) {
                if (thatSchema.equalsIgnoreCase(getScheme()) &&
                        thatAuthority.equals(getUri().getAuthority())) {
                    if (SFMUtil.isValidSFMPath(uri)) {
                        return;
                    }
                }
            } else {
                if (SFMUtil.isValidSFMPath(uri)) {
                    return;
                }
            }
        }

        throw new IllegalArgumentException("Wrong FS: " + path +
                ", expected: " + this.getUri());
    }

    @Override
    public Path makeQualified(Path path) {
        Path absF = fixRelativePart(path);
        return absF.makeQualified(uri, workingDir);
    }

    // path must be absolute path, and will ignore schema and authority automatically
    private synchronized void loadSFMInformation(URI uri) throws IOException {
        String sfmBasePath = SFMUtil.getSFMBasePath(uri);
        if (! sfmBasePath.equals(curSFMBasePath)) {
            SFMetaData metaData = getSFMInformation(sfmBasePath);
            curSFMBasePath = metaData.sfmBasePath;
            curSFMReader = metaData.sfmReader;
            curReadaheadManager = metaData.readaheadManager;
        } else {
            LOG.debug(String.format("%s do not need to reload SFM information.", uri));
        }
    }

    private SFMetaData getSFMInformation(String sfmBasePath) throws IOException {
        LOG.debug(String.format("Start to load SFM information: %s", sfmBasePath));
        SFMetaData metaData = metaDataCache.get(sfmBasePath);
        if (metaData == null) {
            LOG.debug(String.format("SFMIndexReader didn't created, start to build it: %s", sfmBasePath));
            // build SFMIndexReader
            SFMIndexReader sfmReader = SFMIndexReader.build(underLyingFS, sfmBasePath);
            ReadaheadManager readaheadManager = null;
            if (SFMConstants.ENABLE_CACHE) {
                switch (SFMConstants.READAHEAD_MANAGER_ENUM) {
                    case spsa:
                        readaheadManager = new SPSAReadaheadManager(underLyingFS, new Path(sfmBasePath, SFMConstants.MERGED_FILENAME));
                        break;
                    default:
                        readaheadManager = new StaticReadaheadManager(underLyingFS, new Path(sfmBasePath, SFMConstants.MERGED_FILENAME));
                }
            }
            SFMetaData newSFMetaData = new SFMetaData(sfmBasePath, sfmReader, readaheadManager);
            metaDataCache.put(sfmBasePath, newSFMetaData);
            return newSFMetaData;
        } else {
            LOG.debug(String.format("SFMIndexReader already created, just return it: %s", sfmBasePath));
            return metaData;
        }
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

    private class SFMetaData {

        private String sfmBasePath;

        private SFMIndexReader sfmReader;

        private ReadaheadManager readaheadManager;

        public SFMetaData(String sfmBasePath, SFMIndexReader sfmReader) {
            this(sfmBasePath, sfmReader, null);
        }

        public SFMetaData(String sfmBasePath, SFMIndexReader sfmReader, ReadaheadManager readaheadManager) {
            this.sfmBasePath = sfmBasePath;
            this.sfmReader = sfmReader;
            this.readaheadManager = readaheadManager;
        }
    }

}
