package org.inlighting.sfm.gen;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.inlighting.sfm.SFMTestUtils;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class SFMFileGen {

    private byte[] tmpBytes = new byte[100];

    @Test
    void uploadBig() throws IOException {
        final String folder = "/big-articles.sfm";
        final Path qualifiedHDFSPath = SFMTestUtils.genHDFSPath(folder);
        final Path qualifiedSFMPath = SFMTestUtils.genSFMPath(folder);
        final StringBuilder sb = new StringBuilder();
        FileInputStream tmpIn = new FileInputStream("article.txt");
        int read = 0;
        while ((read = tmpIn.read(tmpBytes)) != -1) {
            sb.append(new String(tmpBytes, 0, read));
        }

        FileSystem fs = qualifiedSFMPath.getFileSystem(SFMTestUtils.getDefaultConfiguration());
        FSDataOutputStream out;
        for (int i=0; i<=10000; i++) {
            out = fs.create(new Path(folder, i + ".txt"));
            out.writeBytes(sb.toString());
            out.close();
        }
        fs.close();
    }
}
