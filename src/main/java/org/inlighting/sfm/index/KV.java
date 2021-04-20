package org.inlighting.sfm.index;

import org.inlighting.sfm.merger.FileEntity;

public class KV implements Comparable<KV> {
    private String filename;

    private long offset;

    private int length;

    private long modificationTime;

    private boolean tombstone;

    // used to compare only
    private long nanoTime;

    public KV(String filename, long offset, int length, long modificationTime, boolean tombstone, long nanoTime) {
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.modificationTime = modificationTime;
        this.tombstone = tombstone;
        this.nanoTime = nanoTime;
    }

    public KV(String filename, long offset, int length, long modificationTime, boolean tombstone) {
       this(filename, offset, length, modificationTime, tombstone, 0);
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
    public int compareTo(KV o) {
        int result = filename.compareTo(o.filename);
        if (result == 0) {
            // same filename, compare to nano time.
            result = Long.compare(nanoTime, o.nanoTime);
        }
        return result;
    }
}
