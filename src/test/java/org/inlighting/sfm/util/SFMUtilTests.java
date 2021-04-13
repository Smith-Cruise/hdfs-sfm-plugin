package org.inlighting.sfm.util;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class SFMUtilTests {

    private final Path illegalPath1 = new Path("sam://sin.lab.com:9000/a/b.sfm/");

    @Test
    void getFilenameTests() throws IOException {
        assertThrows(IOException.class, () -> {
            SFMUtil.getFilename("/hello.sfm1/a");
        });

        Path p = new Path("sfm://sin.lab.com:9000/a/b.sfm/a");

        assertEquals("a", SFMUtil.getFilename("/hello.sfm/a"));
        assertEquals("a", SFMUtil.getFilename(p.toUri()));
        p = new Path("sfm://sin.lab.com:9000/a/b.sfm/");
        assertNull(SFMUtil.getFilename(p.toUri()));
        assertNull(SFMUtil.getFilename("/a.sfm"));
        assertThrows(IOException.class, () -> SFMUtil.getFilename(illegalPath1.toUri()));
    }

    @Test
    void getSFMBasePathTests() throws IOException {
        assertThrows(IOException.class, () -> SFMUtil.getSFMBasePath(illegalPath1));
        Path p = new Path("sfm://sin.lab.com:9000/a/hello.sfm/a");
        assertEquals("/a/hello.sfm", SFMUtil.getSFMBasePath(p));
        p = new Path("/hello.sfm/ef");
        assertEquals("/hello.sfm", SFMUtil.getSFMBasePath(p));
    }
}
