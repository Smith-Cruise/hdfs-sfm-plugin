package org.inlighting.sfm.index;

public class MasterIndexIO {

    public static String serialize(MasterIndex masterIndex) {
        return String.format("%d,%d,%s,%s\n", masterIndex.getOffset(), masterIndex.getLength(),
                masterIndex.getMinKey(), masterIndex.getMaxKey());
    }

    public static MasterIndex deserialize(String content) {
        String[] tmp = content.split(",");
        return new MasterIndex(Long.parseLong(tmp[0]), Integer.parseInt(tmp[1]), tmp[2], tmp[3]);
    }
}
