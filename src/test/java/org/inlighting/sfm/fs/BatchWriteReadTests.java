package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.inlighting.sfm.SFMTestUtils;
import org.inlighting.sfm.util.SFMUtil;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

public class BatchWriteReadTests {

    private final String folder = "/batch_write_read_tests.sfm";

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
    void batchWriteAndRandomRead() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out;
            for (int i=0; i<=100; i++) {
                long time = System.currentTimeMillis();
                String filename = time + ".txt";
                out = fs.create(new Path(folder, filename));
                out.writeBytes(filename);
                out.close();
            }
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(qualifiedSFMPath);
            FSDataInputStream in;
            Random random = new Random();
            byte[] bytes = new byte[100];
            for (int i=0; i<10; i++) {
                int randomIndex = random.nextInt(fileStatuses.length);
                FileStatus fileStatus = fileStatuses[randomIndex];
                Path file = fileStatus.getPath();
                String filename = SFMUtil.getFilename(file.toUri());
                in = fs.open(file);
                int read = in.read(bytes);
                assertEquals(filename, new String(bytes, 0, read));
            }
            fs.close();
        }
    }
}
