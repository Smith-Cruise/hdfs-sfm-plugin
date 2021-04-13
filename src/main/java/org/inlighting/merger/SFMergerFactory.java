package org.inlighting.merger;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class SFMergerFactory {

    private SFMergerFactory() {

    }

    public static SFMerger build(FileSystem fs, long blockSize) throws IOException {
        return new SFMerger(fs, blockSize);
    }

}
