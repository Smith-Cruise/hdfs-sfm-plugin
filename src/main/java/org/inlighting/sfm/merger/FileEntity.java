package org.inlighting.sfm.merger;

public class FileEntity implements Comparable<FileEntity> {
    private String sfmBasePath;

    private String filename;

    private String tmpStoreName;

    private long filesSize;

    private long modificationTime;

    private boolean tombstone;

    // used to compare only
    private long nanoTime;

    public FileEntity() {}

    public FileEntity(String sfmBasePath, String filename, String tmpStoreName, long filesSize, long modificationTime,
                      boolean tombstone, long nanoTime) {
        this.sfmBasePath = sfmBasePath;
        this.filename = filename;
        this.tmpStoreName = tmpStoreName;
        this.filesSize = filesSize;
        this.modificationTime = modificationTime;
        this.tombstone = tombstone;
        this.nanoTime = nanoTime;
    }

    public String getSfmBasePath() {
        return sfmBasePath;
    }

    public void setSfmBasePath(String sfmBasePath) {
        this.sfmBasePath = sfmBasePath;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getTmpStoreName() {
        return tmpStoreName;
    }

    public void setTmpStoreName(String tmpStoreName) {
        this.tmpStoreName = tmpStoreName;
    }

    public long getFilesSize() {
        return filesSize;
    }

    public void setFilesSize(long filesSize) {
        this.filesSize = filesSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public void setTombstone(boolean tombstone) {
        this.tombstone = tombstone;
    }

    public long getNanoTime() {
        return nanoTime;
    }

    public void setNanoTime(long nanoTime) {
        this.nanoTime = nanoTime;
    }

    @Override
    public int compareTo(FileEntity o) {
        int result = filename.compareTo(o.filename);
        if (result == 0) {
            // same filename, compare to nano time.
            result = Long.compare(nanoTime, o.nanoTime);
        }
        return result;
    }
}
