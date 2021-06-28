package org.inlighting.sfm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ReadaheadTests {


    @Test
    void read() throws Exception {
        Path path = new Path("sfm://single.lab.com:9000/articles.sfm");
        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus[] fileStatus = fs.listStatus(path);
        Text text = new Text();
        for (int i=0; i<500; i++) {
            FileStatus file = fileStatus[i];
            FSDataInputStream in = fs.open(file.getPath());
            LineReader reader = new LineReader(in);
            int count = 0;
            while (reader.readLine(text) != 0) {
                count++;
            }
            assertEquals(count, 17725);
        }


//        for (int i=2000; i<2300; i++) {
//            FileStatus file = fileStatus[i];
//            FSDataInputStream in = fs.open(file.getPath());
//            LineReader reader = new LineReader(in);
//            int count = 0;
//            while (reader.readLine(text) != 0) {
//                count++;
//            }
//            assertEquals(count, 17725);
//        }
        fs.close();
    }

//    @Test
//    void read() throws Exception {
//        Path path = new Path("sfm://single.lab.com:9000/articles.sfm");
//        FileSystem fs = path.getFileSystem(new Configuration());
//        FileStatus[] fileStatus = fs.listStatus(path);
//        byte[] bytes = new byte[1024*1024];
//        int read;
//        for (int i=0; i<500; i++) {
//            FileStatus file = fileStatus[i];
//            FSDataInputStream in = fs.open(file.getPath());
//            int count = 0;
//            while ((read = in.read(bytes)) != -1) {
//                count+=read;
//            }
//            assertEquals(count, 381486);
//        }
//        fs.close();
//    }
}
