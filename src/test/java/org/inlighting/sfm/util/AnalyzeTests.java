package org.inlighting.sfm.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AnalyzeTests {

    @Test
    void analyze() throws Exception {
        Path path = new Path("hdfs://master.lab.com:9000/1w-articles");
        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus[] statuses = fs.listStatus(path);
        Map<Integer, Integer> files = new LinkedHashMap<>();
        for (int i=100; i<=1500; i+=100) {
            files.put(i, 0);
        }
        for (FileStatus status: statuses) {
            long size = status.getLen();
            int iSize = (int)(size/1024/100);
            int origin = files.get(iSize*100);
            files.put(iSize*100, origin+1);
        }

        for (Map.Entry<Integer, Integer> entry: files.entrySet()) {
            System.out.println(entry.getKey()+":"+(entry.getValue()/10000.0));
        }
    }
}
