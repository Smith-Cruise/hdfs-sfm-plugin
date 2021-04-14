package org.inlighting.sfm.merger;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class SFMergerFactory {

    private SFMergerFactory() {

    }

    public static SFMerger build(FileSystem fs, short replication, long blockSize) throws IOException {
        return new SFMerger(fs, replication, blockSize);
    }

}
