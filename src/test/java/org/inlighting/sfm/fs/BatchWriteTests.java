package org.inlighting.sfm.fs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class BatchWriteTests {
//    private final String basePath = "/test.sfm";
//
//    private final Path hdfsPath = new Path("hdfs://single.lab.com:9000/batch.sfm");
//
//    private final Path sfmPath = new Path("sfm://single.lab.com:9000/batch.sfm");
//
//    private final StringBuilder stringBuilder = new StringBuilder();
//
//    @BeforeEach
//    void deleteTestSFM() throws IOException {
//        FileSystem fs = hdfsPath.getFileSystem(HDFSConfiguration.getDefaultConfiguration());
//        if (fs.exists(hdfsPath)) {
//            fs.delete(hdfsPath, true);
//        }
//
//        FileInputStream in = new FileInputStream("content.txt");
//        byte[] tmp = new byte[100];
//        int read = 0;
//        while ((read = in.read(tmp)) != -1) {
//            stringBuilder.append(new String(tmp, 0, read));
//        }
//    }
//
//    @Test
//    void batchWrite() throws IOException {
//        {
//            FileSystem fs = HDFSConfiguration.getSFMFileSystem(sfmPath.toUri());
//            FSDataOutputStream out;
//            for (int i=0; i<=1000; i++) {
//                String filename = System.currentTimeMillis() + ".txt";
//                out = fs.create(new Path("/batch.sfm/" + filename));
//                out.writeBytes(stringBuilder.toString());
//                out.close();
//            }
//            fs.close();
//        }
//    }
}
