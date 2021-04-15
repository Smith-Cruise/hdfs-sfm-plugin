package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WriteReadTests {

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
    }

    // 一次常规读写
    @Test
    void singleWriteRead() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
            int result = in.readInt();
            in.close();
            assertEquals(5, result);

            // test not found file
            assertThrows(FileNotFoundException.class, () -> {
                fs.open(new Path(sfmBasePath + "/c.txt"));
            });
            fs.close();
        }
    }

    // 写入两次，第二次覆盖第一次的 filename，读取应该读取到第二次的内容（最新），中间close过
    @Test
    void doubleWriteSingleRead1() throws IOException {
        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeBytes("hello");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
            byte[] tmp = new byte[10];
            int read = in.read(tmp);
            in.close();
            byte[] str = new byte[read];
            System.arraycopy(tmp, 0, str, 0, read);
            assertEquals("hello", new String(str));
            fs.close();
        }
    }

    // 写入两次，第二次覆盖第一次的 filename，读取应该读取到第二次的内容（最新），中间不close
    @Test
    void doubleWriteSingleRead2() throws IOException {
        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeBytes("hello");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
            byte[] tmp = new byte[10];
            int read = in.read(tmp);
            in.close();
            byte[] str = new byte[read];
            System.arraycopy(tmp, 0, str, 0, read);
            assertEquals("hello", new String(str));
            fs.close();
        }
    }

    // 创建两次，每次不同的 filename，两个都能正常读取。
    @Test
    void doubleWriteDoubleRead() throws IOException {
        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/b.txt"));
            out.writeBytes("hello");
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
            assertEquals(5, in.readInt());
            in.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/b.txt"));
            byte[] tmp = new byte[10];
            int read = in.read(tmp);
            in.close();
            byte[] str = new byte[read];
            System.arraycopy(tmp, 0, str, 0, read);
            assertEquals("hello", new String(str));
            fs.close();
        }
    }

    @Test
    void writeDeleteRead() throws IOException {
        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
            out.writeInt(5);
            out.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
            assertEquals(5, in.readInt());
            in.close();
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            fs.delete(new Path(sfmBasePath + "/a.txt"), false);
            fs.close();
        }

        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            assertThrows(FileNotFoundException.class, () -> fs.open(new Path(sfmBasePath + "/a.txt")));
            fs.close();
        }
    }

    @Test
    void listFiles() throws IOException {
        {
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
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
            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(sfmBasePath));
            assertEquals(2, fileStatuses.length);
            assertEquals("HelloWorld".getBytes(StandardCharsets.UTF_8).length, fileStatuses[1].getLen());
            assertEquals("a.txt".getBytes(StandardCharsets.UTF_8).length, fileStatuses[0].getLen());
            fs.close();
        }

    }
}
