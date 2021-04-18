package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class SFMStreamTests {

    private final String folder = "/sfm_stream_tests.sfm";

    private final Path qualifiedHDFSPath = SFMTestUtils.genHDFSPath(folder);

    private final Path qualifiedSFMPath = SFMTestUtils.genSFMPath(folder);

    private final Configuration hdfsConfiguration = SFMTestUtils.getDefaultConfiguration();

    @BeforeEach
    void deleteTestSFM() throws IOException {
        FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
        if (fs.exists(qualifiedHDFSPath)) {
            fs.delete(qualifiedHDFSPath, true);
        }
    }

    @Test
    void seekTest() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out;
            out = fs.create(new Path(folder, "a.txt"));
            out.writeInt(4);
            out.close();

            out = fs.create(new Path(folder, "b.txt"));
            out.writeInt(6);
            out.close();

            out = fs.create(new Path(folder, "c.txt"));
            out.writeInt(10);
            out.close();

            fs.close();


        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in;
            in = fs.open(new Path(folder, "b.txt"));
            for (int i=0; i<4; i++) {
                in.seek(i);
            }
            assertThrows(IOException.class, () -> in.seek(5)) ;
            in.close();
            fs.close();
        }
    }

}
