package org.inlighting.sfm.uploader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Uploader {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        String localFolder = "/home/smith/1w-articles/1w-articles";
        String hdfsFolder = "/1w-test.sfm";
        Path path = new Path("sfm://master.lab.com:9000"+hdfsFolder);
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = path.getFileSystem(configuration);
        File dir = new File(localFolder);
        if (! dir.isDirectory()) {
            return;
        }
        File[] files = dir.listFiles();
        for (File file: files) {
            Path tmp = new Path(hdfsFolder+"/"+file.getName());
            FSDataOutputStream out = fs.create(tmp);
            write(out, file);
            out.close();
        }
        fs.close();
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime);
    }

    private static void write(FSDataOutputStream out, File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        byte[] buffer = new byte[128];
        int read;
        while ((read = inputStream.read(buffer)) > 0) {
            out.write(buffer, 0, read);
        }
    }
}
