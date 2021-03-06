package org.inlighting.sfm.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.readahead.ReadaheadManager;

import java.io.IOException;

public class SFMFsDataInputStream extends FSDataInputStream {
    public SFMFsDataInputStream(FileSystem fs, Path p, long start, long length, int bufferSize, ReadaheadManager readaheadManager) throws IOException {
        super(new SFMFsInputStream(fs, p, start, length, bufferSize, readaheadManager));
    }
}
