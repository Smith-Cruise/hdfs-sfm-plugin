package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class SFMFileSystemTests {

    private final String authority = "single.lab.com:9000";

    private final String sfmBasePath = "/test.sfm";

    private final Path qualifiedHDFSPath = new Path("hdfs://"+authority+sfmBasePath);

    private final Path qualifiedSFMPath = new Path("sfm://"+authority+sfmBasePath);

    private static final Configuration hdfsConfiguration = new Configuration();

    @BeforeAll
    static void loadHdfsConfiguration() {
        hdfsConfiguration.set("dfs.replication", "1");
    }

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
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/b.txt"));
            out.writeBytes("Hello World");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus dir1 = fs.getFileStatus(new Path(sfmBasePath));
            FileStatus dir2 = fs.getFileStatus(qualifiedSFMPath);
            FileStatus a = fs.getFileStatus(new Path(sfmBasePath + "/a.txt"));
            FileStatus b = fs.getFileStatus(new Path(sfmBasePath + "/b.txt"));

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
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();

            out = fs.create(new Path(sfmBasePath + "/delete.txt"));
            out.writeBytes("HelloWorld");
            out.close();

            out = fs.create(new Path(sfmBasePath + "/b.txt"));
            out.writeBytes("HelloWorld");
            out.close();

            out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeBytes("a.txt");
            out.close();

            fs.delete(new Path(sfmBasePath + "/delete.txt"), false);
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(sfmBasePath));
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
        assertEquals("sfm://"+authority+"/hello", fs.makeQualified(path).toString());
        fs.close();
    }
}
