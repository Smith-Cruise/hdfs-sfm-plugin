package org.inlighting.sfm.fs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.inlighting.sfm.merger.SFMerger;

import java.io.IOException;

public class SFMFsDataOutputStream extends FSDataOutputStream  {
    public SFMFsDataOutputStream(SFMerger SFMerger, String indexName, String filename, FileSystem.Statistics stat) throws IOException {
        super(new SFMOutputStream(SFMerger, indexName, filename), stat);
    }
}
