package org.inlighting.sfm.fs;

import org.apache.hadoop.fs.*;
import org.inlighting.sfm.HdfsConfiguration;
import org.inlighting.sfm.util.SFMUtil;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

public class BatchWriteReadTests {


    private final Path hdfsPath = new Path("hdfs://single.lab.com:9000/batch.sfm");

    private final Path sfmPath = new Path("sfm://single.lab.com:9000/batch.sfm");


    @BeforeEach
    void deleteTestSFM() throws IOException {
        FileSystem fs = hdfsPath.getFileSystem(HdfsConfiguration.getDefault());
        if (fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, true);
        }
    }

    @Test
    void batchWriteAndRandomRead() throws IOException {
        {
            FileSystem fs = sfmPath.getFileSystem(HdfsConfiguration.getDefault());
            FSDataOutputStream out;
            for (int i=0; i<=1000; i++) {
                long time = System.currentTimeMillis();
                String filename = time + ".txt";
                out = fs.create(new Path("/batch.sfm/" + filename));
                out.writeBytes(filename);
                out.close();
            }
            fs.close();
        }

        {
            FileSystem fs = sfmPath.getFileSystem(HdfsConfiguration.getDefault());
            FileStatus[] fileStatuses = fs.listStatus(sfmPath);
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
