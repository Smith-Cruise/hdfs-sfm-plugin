package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ReadaheadTests {


    @Test
    void read() throws Exception {
        Path path = new Path("sfm://single.lab.com:9000/articles.sfm");
        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i=0; i<100; i++) {
            FileStatus file = fileStatus[i];
            FSDataInputStream in = fs.open(file.getPath());
            byte[] bytes = new byte[(int)file.getLen()];
            int read = in.read(bytes);
            assertEquals(read, (int)file.getLen());
        }
        fs.close();
    }
}
