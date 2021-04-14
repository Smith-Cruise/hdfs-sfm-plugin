package org.inlighting.sfm.util;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;

public class SFMUtilTests {

    private final Path illegalPath1 = new Path("sam://sin.lab.com:9000/a/b.sfm/");

    @Test
    void isValidSFMURITests() {
        Path path1 = new Path("sfm://single.lab.com:9000/a/b/c.sfm/b");
        Path path2 = new Path("a/b/c.sfm/b");
        Path path3 = new Path("a/b/c.s1m/b");
        assertTrue(SFMUtil.isValidSFMPath(path1.toUri()));
        assertTrue(SFMUtil.isValidSFMPath(path2.toUri()));
        assertFalse(SFMUtil.isValidSFMPath(path3.toUri()));
    }

    @Test
    void getFilenameTests() throws IOException {
        Path p = new Path("sfm://sin.lab.com:9000/a/b.sfm/a");
        assertEquals("a", SFMUtil.getFilename(p.toUri()));
        p = new Path("sfm://sin.lab.com:9000/a/b.sfm/");
        assertNull(SFMUtil.getFilename(p.toUri()));
        assertNull(SFMUtil.getFilename(URI.create("/a.sfm")));
        assertEquals("a", SFMUtil.getFilename(URI.create("b.sfm/a")));
    }

    @Test
    void getSFMBasePathTests() throws IOException {
        Path p = new Path("sfm://sin.lab.com:9000/a/hello.sfm/a");
        assertEquals("/a/hello.sfm", SFMUtil.getSFMBasePath(p.toUri()));
        p = new Path("/hello.sfm/ef");
        assertEquals("/hello.sfm", SFMUtil.getSFMBasePath(p.toUri()));
    }
}
