package org.inlighting.sfm.index;

public class KV {
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

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
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

    public boolean isTombstone() {
        return tombstone;
    }

    public void setTombstone(boolean tombstone) {
        this.tombstone = tombstone;
    }
}
