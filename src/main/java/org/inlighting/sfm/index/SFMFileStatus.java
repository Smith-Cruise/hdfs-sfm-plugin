package org.inlighting.sfm.index;

public class SFMFileStatus {

    private String filename;

    private String mergedFilename;

    private long offset;

    private int length;

    long modificationTime;

    public SFMFileStatus(String filename, String mergedFilename, long offset, int length, long modificationTime) {
        this.filename = filename;
        this.mergedFilename = mergedFilename;
        this.offset = offset;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getMergedFilename() {
        return mergedFilename;
    }

    public void setMergedFilename(String mergedFilename) {
        this.mergedFilename = mergedFilename;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }
}
