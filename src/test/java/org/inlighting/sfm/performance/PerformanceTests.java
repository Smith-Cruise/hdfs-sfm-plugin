package org.inlighting.sfm.performance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMTestUtils;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class PerformanceTests {

    private byte[] tmpBytes = new byte[100];

    @Test
    void createNormalArticles() throws IOException {
        final int times = 3000;

        final String folder = "/3000-articles";
        final Path qualifiedHDFSPath = SFMTestUtils.genHDFSPath(folder);
//        final Path qualifiedSFMPath = SFMTestUtils.genSFMPath(folder);
        final StringBuilder sb = new StringBuilder();
        FileInputStream tmpIn = new FileInputStream("article.txt");
        int read = 0;
        while ((read = tmpIn.read(tmpBytes)) != -1) {
            sb.append(new String(tmpBytes, 0, read));
        }

        FileSystem fs = qualifiedHDFSPath.getFileSystem(SFMTestUtils.getDefaultConfiguration());
        FSDataOutputStream out;
        for (int i=0; i<times; i++) {
            out = fs.create(new Path(folder, i + ".txt"));
            out.writeBytes(sb.toString());
            out.close();
            if (i%100 == 0) {
                System.out.println(i);
            }
        }
        fs.close();
    }

    @Test
    void createSFMArticles() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        final int times = 3000;
        final String folder = "/articles.sfm";
        final Path qualifiedSFMPath = SFMTestUtils.genSFMPath("master.lab.com:9000", folder);
        final StringBuilder sb = new StringBuilder();
        FileInputStream tmpIn = new FileInputStream("article.txt");
        int read = 0;
        while ((read = tmpIn.read(tmpBytes)) != -1) {
            sb.append(new String(tmpBytes, 0, read));
        }

        FileSystem fs = qualifiedSFMPath.getFileSystem(configuration);
        FSDataOutputStream out;
        for (int i=0; i<times; i++) {
            out = fs.create(new Path(folder, i + ".txt"));
            out.writeBytes(sb.toString());
            out.close();
            if (i%100 == 0) {
                System.out.println(i);
            }
        }
        fs.close();
    }
}
