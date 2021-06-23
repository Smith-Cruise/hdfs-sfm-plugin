package org.inlighting.sfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SFMTestUtils {

    private final static String AUTHORITY = "master.lab.com:9000";

    public static Path genSFMPath(String folderPath) {
        return new Path("sfm", AUTHORITY, folderPath);
    }

    public static Path genSFMPath(String authority, String folderPath) {
        return new Path("sfm", authority, folderPath);
    }

    public static Path genHDFSPath(String folderPath) {
        return new Path("hdfs", AUTHORITY, folderPath);
    }

    public static Configuration getDefaultConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        return configuration;
    }
}
