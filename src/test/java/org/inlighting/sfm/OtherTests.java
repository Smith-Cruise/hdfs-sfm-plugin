package org.inlighting.sfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class OtherTests {

    @Test
    void test() {
        System.out.println(System.currentTimeMillis());
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime());
        System.out.println(System.currentTimeMillis());

    }

    @Test
    void tmpTest() throws IOException {
        Path qualifiedSFMPath = new Path("hdfs://single.lab.com:9000/CentOS-7-x86_64-Minimal-2009.iso");
        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
        BlockLocation[] blockLocations = fs.getFileBlockLocations(qualifiedSFMPath, 0, 1342177280);
        for (BlockLocation location: blockLocations) {
            System.out.println(location.toString());
        }
        fs.close();
    }

    @Test
    void tmpRead() throws IOException {
        Path qualifiedSFMPath = new Path("sfm://single.lab.com:9000/batch.sfm/1618298781195.txt");
        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
        fs.open(qualifiedSFMPath);
        System.out.println(fs.exists(qualifiedSFMPath));
        fs.close();
    }

    @Test
    void listRead() throws IOException {
        Path qualifiedSFMPath = SFMTestUtils.genSFMPath("/batch_write_read_tests.sfm");
        FileSystem fs = qualifiedSFMPath.getFileSystem(SFMTestUtils.getDefaultConfiguration());
        FileStatus[] fileStatuses = fs.listStatus(qualifiedSFMPath);
        System.out.println(fileStatuses.length);
//        for (FileStatus fileStatus: fileStatuses) {
//            System.out.println(fileStatus.toString());
//        }
        fs.close();
    }
}
