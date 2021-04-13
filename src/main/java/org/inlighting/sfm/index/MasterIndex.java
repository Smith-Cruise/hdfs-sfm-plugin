package org.inlighting.sfm.index;

public class MasterIndex {

    private long offset;

    private int length;

    private String minKey;

    private String maxKey;

    public MasterIndex(long offset, int length, String minKey, String maxKey) {
        this.offset = offset;
        this.length = length;
        this.minKey = minKey;
        this.maxKey = maxKey;
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

    public String getMinKey() {
        return minKey;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }
}
