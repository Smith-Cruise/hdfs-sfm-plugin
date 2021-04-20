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

    @Test
    void tmpTest() throws IOException {
        Path qualifiedSFMPath = new Path("hdfs://single.lab.com:9000/CentOS-7-x86_64-Minimal-2009.iso");
        DFSClient client = new DFSClient(qualifiedSFMPath.toUri(), new Configuration());
        LocatedBlocks locatedBlocks = client.getLocatedBlocks("/CentOS-7-x86_64-Minimal-2009.iso", 0);
        List<LocatedBlock> list = locatedBlocks.getLocatedBlocks();
        System.out.println("-----");
        for (LocatedBlock b: list) {
            System.out.println(b);
        }
        System.out.println(locatedBlocks.toString());
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
//       System.out.println("99.txt".compareTo("351.txt"));
        List<FileEntity> fileEntities = new ArrayList<>(500);
//        fileEntities.add(new FileEntity(null, "article.txt", null, 0, 0, true, System.nanoTime()));
        for (int i=0; i<=1000; i++) {
            fileEntities.add(new FileEntity(null, i+".txt", null, 0, 0, true, System.nanoTime()));
        }

        String lastKey = null;
        long lastNanoTime = 0;

        for (FileEntity fileEntity: fileEntities) {
            if (lastKey != null) {
                int res = lastKey.compareTo(fileEntity.getFilename());
                if (res > 0) {
                    throw new IOException(String.format("The key should be ordered. Last key: %s, now key: %s", lastKey, fileEntity.getFilename()));
                } else if (res == 0) {
                    if (lastNanoTime >= fileEntity.getNanoTime()) {
                        throw new IOException(String.format("Have the same key, but the nano time should be ordered. Last key: %s, now key: %s", lastKey, fileEntity.getNanoTime()));
                    }
                }
            }

            lastKey = fileEntity.getFilename();
            lastNanoTime = fileEntity.getNanoTime();
        }
    }

}
