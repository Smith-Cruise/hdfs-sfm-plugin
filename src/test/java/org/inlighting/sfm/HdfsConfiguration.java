package org.inlighting.sfm;

import org.apache.hadoop.conf.Configuration;

public class HdfsConfiguration {

    public static Configuration getDefault() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        return configuration;
    }
}
