package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class SFMFileSystemTests {

    private final String authority = "single.lab.com:9000";

    private final String sfmBasePath = "/test.sfm";

    private final Path hdfsPath = new Path("hdfs://"+authority+sfmBasePath);

    private final Path sfmPath = new Path("sfm://"+authority+sfmBasePath);

    private static final Configuration hdfsConfiguration = new Configuration();

    @BeforeAll
    static void loadHdfsConfiguration() {
        hdfsConfiguration.set("dfs.replication", "1");
    }

    @BeforeEach
    void deleteTestSFM() throws IOException {
        FileSystem fs = hdfsPath.getFileSystem(hdfsConfiguration);
        if (fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, true);
        }
        fs.close();
    }

    @Test
    void getFileStatusTest() throws IOException {
        {
            FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/b.txt"));
            out.writeBytes("Hello World");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
            FileStatus dir = fs.getFileStatus(new Path(sfmBasePath));
            FileStatus a = fs.getFileStatus(new Path(sfmBasePath + "/a.txt"));
            FileStatus b = fs.getFileStatus(new Path(sfmBasePath + "/b.txt"));

            assertTrue(dir.isDirectory());
            assertTrue(a.isFile());
            assertTrue(a.isFile());

            assertEquals(0, dir.getLen());
            assertEquals(4, a.getLen());
            assertEquals("Hello World".getBytes(StandardCharsets.UTF_8).length, b.getLen());

            assertEquals(sfmPath.toString(), dir.getPath().toString());
            assertEquals(sfmPath+"/a.txt", a.getPath().toString());
            assertEquals(sfmPath+"/b.txt", b.getPath().toString());

            fs.close();
        }
    }

    @Test
    void listStatusTest() throws IOException {
        {
            FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
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
            FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(sfmPath);
            assertEquals(2, fileStatuses.length);
            assertEquals("a.txt".getBytes(StandardCharsets.UTF_8).length, fileStatuses[1].getLen());
            assertEquals(sfmPath+"/a.txt", fileStatuses[1].getPath().toString());
            assertEquals("HelloWorld".getBytes(StandardCharsets.UTF_8).length, fileStatuses[0].getLen());
            assertEquals(sfmPath+"/b.txt", fileStatuses[0].getPath().toString());
            fs.close();
        }

    }

    @Test
    void makeQualifiedTest() throws IOException {
        FileSystem fs = sfmPath.getFileSystem(hdfsConfiguration);
        Path path = new Path("/hello");
        assertEquals("sfm://"+authority+"/hello", fs.makeQualified(path).toString());
    }
}
