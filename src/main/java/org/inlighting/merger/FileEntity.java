package org.inlighting.merger;

public class FileEntity implements Comparable<FileEntity> {
    private String sfmBasePath;

    private String filename;

    private String tmpStoreName;

    private long filesSize;

    private boolean tombstone;

    public FileEntity() {}

    public FileEntity(String sfmBasePath, String filename, String tmpStoreName, long filesSize, boolean tombstone) {
        this.sfmBasePath = sfmBasePath;
        this.filename = filename;
        this.tmpStoreName = tmpStoreName;
        this.filesSize = filesSize;
        this.tombstone = tombstone;
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

    public boolean isTombstone() {
        return tombstone;
    }

    public void setTombstone(boolean tombstone) {
        this.tombstone = tombstone;
    }

    @Override
    public int compareTo(FileEntity o) {
        return filename.compareTo(o.filename);
    }
}
