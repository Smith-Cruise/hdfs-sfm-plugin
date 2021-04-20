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
    void test() {
        System.out.println(System.currentTimeMillis());
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime());
        System.out.println(System.currentTimeMillis());

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
        Path qualifiedSFMPath = SFMTestUtils.genSFMPath("master.lab.com:9000", "/articles.sfm");
        FileSystem fs = qualifiedSFMPath.getFileSystem(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(new Path("/articles.sfm"));
        int offset = 0;
        int loop = 0;
        for (FileStatus fileStatus: fileStatuses) {
            loop ++;
            BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            boolean meet = false;
            for(int i = 0; i < blkLocations.length; ++i) {
                if (blkLocations[i].getOffset() <= offset && offset < blkLocations[i].getOffset() + blkLocations[i].getLength()) {
                    meet = true;
                }
            }
            if (meet) {
                continue;
            }

            BlockLocation last = blkLocations[blkLocations.length - 1];
            long fileLength = last.getOffset() + last.getLength() - 1L;
            throw new IllegalArgumentException("Offset " + offset + " is outside of file (0.." + fileLength + ")");
        }

    }

}
