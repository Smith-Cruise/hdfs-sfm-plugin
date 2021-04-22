package org.inlighting.sfm.cache;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class SFMCacheFactory {

    public static SFMCacheManager build(FileSystem fs, Path mergedPath) {
        try {
            return new SFMCacheManager(fs, mergedPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
