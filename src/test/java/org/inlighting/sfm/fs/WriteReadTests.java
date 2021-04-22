package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.inlighting.sfm.SFMTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WriteReadTests {

    private final String folder = "/write_read_tests.sfm";

    private final Path qualifiedHDFSPath = SFMTestUtils.genHDFSPath(folder);

    private final Path qualifiedSFMPath = SFMTestUtils.genSFMPath(folder);

    private final Configuration hdfsConfiguration = SFMTestUtils.getDefaultConfiguration();

    private byte[] tmpBytes = new byte[100];

    @BeforeEach
    void deleteTestSFM() throws IOException {
        FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
        if (fs.exists(qualifiedHDFSPath)) {
            fs.delete(qualifiedHDFSPath, true);
        }
    }

    // 简单读写测试
    @Test
    void simpleReadWriteTest() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(folder, "a.txt"));
            out.writeInt(5);
            out.close();
            out = fs.create(new Path(folder, "content1.txt"));
            out.writeBytes("Hello World");
            out.writeInt(9);
            out.close();
            out = fs.create(new Path(folder,"content2.txt"));
            out.writeBytes("World Hello");
            out.close();
            out = fs.create(new Path(folder, "b.txt"));
            out.writeDouble(10.123);
            out.close();
            fs.close();
        }

        {
            int read = 0;

            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataInputStream in = fs.open(new Path(folder, "a.txt"));
            assertEquals(5, in.readInt());
            in.close();

            in = fs.open(new Path(folder, "content1.txt"));
            read = in.read(tmpBytes, 0, 11);
            assertEquals("Hello World", new String(tmpBytes, 0, read));
            assertEquals(4, in.available());
            assertEquals(9, in.readInt());
            assertEquals(0, in.available());
            in.seek(11);
            assertEquals(9, in.readInt());
            in.seek(0);
            assertEquals(11, in.skip(11));
            assertEquals(9, in.readInt());
            assertEquals(15, in.getPos());

            FSDataInputStream finalIn = in;
            assertThrows(EOFException.class, ()-> {
                finalIn.readFully(0, tmpBytes);
            });


            in = fs.open(new Path(folder, "content2.txt"));
            read = in.read(tmpBytes, 0, 11);
            assertEquals("World Hello", new String(tmpBytes, 0, read));

            in = fs.open(new Path(folder, "b.txt"));
            assertEquals(10.123, in.readDouble());


            // test not found file
            assertThrows(FileNotFoundException.class, () -> {
                fs.open(new Path(folder + "/c.txt"));
            });
            fs.close();
        }
    }

    // 写入两次，第二次覆盖第一次的 filename，读取应该读取到第二次的内容（最新），中间close过
    @Test
    void complexReadWriteTest() throws IOException {
        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FSDataOutputStream out = fs.create(new Path(folder + "/a.txt"));
            out.write(1);
            out.close();

            out = fs.create(new Path(folder + "/c.txt"));
            out.writeFloat(2);
            out.close();

            out = fs.create(new Path(folder + "/delete.txt"));
            out.writeBytes("delete");
            out.close();

            out = fs.create(new Path(folder + "/b.txt"));
            out.writeDouble(3);
            out.close();

            fs.delete(new Path(folder + "/delete.txt"), false);

            out = fs.create(new Path(folder + "/a.txt"));
            out.writeInt(999);
            out.close();

            fs.close();
        }

        {
            FileSystem fs = qualifiedSFMPath.getFileSystem(hdfsConfiguration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(folder));
            assertEquals(3, fileStatuses.length);


            FSDataInputStream in = fs.open(new Path(folder + "/a.txt"));
            assertEquals(999, in.readInt());
            in.close();

            in = fs.open(new Path(folder + "/c.txt"));
            assertEquals(2, in.readFloat());
            in.close();

            in = fs.open(new Path(folder + "/b.txt"));
            assertEquals(3, in.readDouble());
            in.close();

            assertThrows(FileNotFoundException.class, ()-> fs.open(new Path(folder + "/delete.txt")));

            fs.close();
        }
    }


//
//    // 写入两次，第二次覆盖第一次的 filename，读取应该读取到第二次的内容（最新），中间不close
//    @Test
//    void doubleWriteSingleRead2() throws IOException {
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeInt(5);
//            out.close();
//            out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeBytes("hello");
//            out.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
//            byte[] tmp = new byte[10];
//            int read = in.read(tmp);
//            in.close();
//            byte[] str = new byte[read];
//            System.arraycopy(tmp, 0, str, 0, read);
//            assertEquals("hello", new String(str));
//            fs.close();
//        }
//    }
//
//    // 创建两次，每次不同的 filename，两个都能正常读取。
//    @Test
//    void doubleWriteDoubleRead() throws IOException {
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeInt(5);
//            out.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/b.txt"));
//            out.writeBytes("hello");
//            out.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
//            assertEquals(5, in.readInt());
//            in.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/b.txt"));
//            byte[] tmp = new byte[10];
//            int read = in.read(tmp);
//            in.close();
//            byte[] str = new byte[read];
//            System.arraycopy(tmp, 0, str, 0, read);
//            assertEquals("hello", new String(str));
//            fs.close();
//        }
//    }
//
//    @Test
//    void writeDeleteRead() throws IOException {
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeInt(5);
//            out.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataInputStream in = fs.open(new Path(sfmBasePath + "/a.txt"));
//            assertEquals(5, in.readInt());
//            in.close();
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            fs.delete(new Path(sfmBasePath + "/a.txt"), false);
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            assertThrows(FileNotFoundException.class, () -> fs.open(new Path(sfmBasePath + "/a.txt")));
//            fs.close();
//        }
//    }
//
//    @Test
//    void listFiles() throws IOException {
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FSDataOutputStream out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeInt(5);
//            out.close();
//
//            out = fs.create(new Path(sfmBasePath + "/delete.txt"));
//            out.writeBytes("HelloWorld");
//            out.close();
//
//            out = fs.create(new Path(sfmBasePath + "/b.txt"));
//            out.writeBytes("HelloWorld");
//            out.close();
//
//            out = fs.create(new Path(sfmBasePath + "/a.txt"));
//            out.writeBytes("a.txt");
//            out.close();
//
//            fs.delete(new Path(sfmBasePath + "/delete.txt"), false);
//            fs.close();
//        }
//
//        {
//            FileSystem fs = qualifiedHDFSPath.getFileSystem(hdfsConfiguration);
//            FileStatus[] fileStatuses = fs.listStatus(new Path(sfmBasePath));
//            assertEquals(2, fileStatuses.length);
//            assertEquals("HelloWorld".getBytes(StandardCharsets.UTF_8).length, fileStatuses[1].getLen());
//            assertEquals("a.txt".getBytes(StandardCharsets.UTF_8).length, fileStatuses[0].getLen());
//            fs.close();
//        }
//
//    }
}
