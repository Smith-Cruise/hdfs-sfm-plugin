package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.inlighting.sfm.SFMTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class SFMFileSystemTests {

    private final String folderPath = "/sfm_filesystem_tests.sfm";

    private final Path qualifiedHDFSPath = SFMTestUtils.genHDFSPath(folderPath);

    private final Path qualifiedSFMPath = SFMTestUtils.genSFMPath(folderPath);

    private final Configuration hdfsConfiguration = SFMTestUtils.getDefaultConfiguration();

    @BeforeEach
    void deleteTestSFM() throws IOException {
        FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
        if (fs.exists(qualifiedHDFSPath)) {
            fs.delete(qualifiedHDFSPath, true);
        }
        fs.close();
    }

    @Test
    void getFileStatusTest() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(folderPath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(folderPath + "/b.txt"));
            out.writeBytes("Hello World");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus dir1 = fs.getFileStatus(new Path(folderPath));
            FileStatus dir2 = fs.getFileStatus(qualifiedSFMPath);
            FileStatus a = fs.getFileStatus(new Path(folderPath + "/a.txt"));
            FileStatus b = fs.getFileStatus(new Path(folderPath + "/b.txt"));

            assertTrue(dir1.isDirectory());
            assertTrue(a.isFile());
            assertTrue(a.isFile());

            assertEquals(0, dir1.getLen());
            assertEquals(4, a.getLen());
            assertEquals("Hello World".getBytes(StandardCharsets.UTF_8).length, b.getLen());

            assertEquals(qualifiedSFMPath.toString(), dir1.getPath().toString());
            assertEquals(qualifiedSFMPath.toString(), dir2.getPath().toString());
            assertEquals(qualifiedSFMPath+"/a.txt", a.getPath().toString());
            assertEquals(qualifiedSFMPath+"/b.txt", b.getPath().toString());

            fs.close();
        }


    }

    @Test
    void listStatusTest() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(folderPath + "/a.txt"));
            out.writeInt(5);
            out.close();

            out = fs.create(new Path(folderPath + "/delete.txt"));
            out.writeBytes("HelloWorld");
            out.close();

            out = fs.create(new Path(folderPath + "/b.txt"));
            out.writeBytes("HelloWorld");
            out.close();

            out = fs.create(new Path(folderPath + "/a.txt"));
            out.writeBytes("a.txt");
            out.close();

            fs.delete(new Path(folderPath + "/delete.txt"), false);
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(folderPath));
            assertEquals(2, fileStatuses.length);
            assertEquals("a.txt".getBytes(StandardCharsets.UTF_8).length, fileStatuses[1].getLen());
            assertEquals(qualifiedSFMPath+"/a.txt", fileStatuses[1].getPath().toString());
            assertEquals("HelloWorld".getBytes(StandardCharsets.UTF_8).length, fileStatuses[0].getLen());
            assertEquals(qualifiedSFMPath+"/b.txt", fileStatuses[0].getPath().toString());
            fs.close();
        }
    }

    @Test
    void makeQualifiedTest() throws IOException {
        FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
        Path path = new Path("/hello");
        assertEquals(SFMTestUtils.genSFMPath("/hello").toString(), fs.makeQualified(path).toString());
        fs.close();
    }
}
