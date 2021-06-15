package org.inlighting.sfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.inlighting.sfm.merger.FileEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OtherTests {

    @Test
    void test() throws Exception {
        Path path = new Path("sfm://single.lab.com:9000/articles.sfm");
        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(path);
        int length = fileStatuses.length;
        for (int i=0; i<length; i+=10) {
            FileStatus fileStatus = fileStatuses[i];
            System.out.println(fileStatus.getLen());
            byte[] bytes = new byte[(int)fileStatus.getLen()];
            Path fileStatusPath = fileStatus.getPath();
            FSDataInputStream inputStream = fs.open(fileStatusPath);
            inputStream.readFully(0, bytes);
            inputStream.close();
        }
    }

//    @Test
//    void tmpTest() throws IOException {
//        Path qualifiedSFMPath = new Path("hdfs://master.lab.com:9000/CentOS-7-x86_64-Minimal-2009.iso");
////        DFSClient client = new DFSClient(qualifiedSFMPath.toUri(), new Configuration());
////        LocatedBlocks locatedBlocks = client.getLocatedBlocks("/CentOS-7-x86_64-Minimal-2009.iso", 0);
////        List<LocatedBlock> list = locatedBlocks.getLocatedBlocks();
////        System.out.println("-----");
////        for (LocatedBlock b: list) {
////            System.out.println(b);
////        }
////        System.out.println(locatedBlocks.toString());
//        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
//        FSDataOutputStream out = fs.append(new Path("hdfs://master.lab.com:9000/CentOS-7-x86_64-Minimal-2009.iso"));
//        System.out.println(out.getPos());
//    }

    @Test
    void tmpTest() throws IOException {
        Path qualifiedSFMPath = new Path("sfm://master.lab.com:9000/articles.sfm/2999.txt");
        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
//        BlockLocation[] blockLocations = fs.getFileBlockLocations(new Path("/articles.sfm/2999.txt"),0, 1);
//        for (BlockLocation blockLocation: blockLocations) {
//            System.out.println(blockLocation);
//        }
        fs.open(new Path("/articles.sfm/2999.txt"));
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
        Path qualifiedSFMPath = SFMTestUtils.genSFMPath("single.lab.com:9000", "/articles.sfm");
        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(new Path("/articles.sfm"));
        for (FileStatus fileStatus: fileStatuses) {
            System.out.println(fileStatus.getPath().toUri().getPath());
        }

    }

}
