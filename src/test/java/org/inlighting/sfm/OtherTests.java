package org.inlighting.sfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class OtherTests {

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
}
