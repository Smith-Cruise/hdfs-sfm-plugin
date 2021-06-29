package org.inlighting.sfm.gen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class ArticlesGenerator {

    // 128 bytes
    final static String msg = "do not go gentle into that good night. old age should burn. rage rage, to burn that light, hello world, i am fine thanksk and y\n";
    final static byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws IOException {
        int articlesNum = 3;
        File outFolder = new File("out");
        if (!outFolder.exists()) {
            outFolder.mkdir();
        }
        for (int i=1; i<=articlesNum; i++) {
            File file = new File("out", i+".txt");
            FileOutputStream outputStream = new FileOutputStream(file);
            writeRandomData(outputStream);
            outputStream.close();
        }
    }

    private static void writeRandomData(OutputStream outputStream) throws IOException {
        int randomTimes = new Random(System.currentTimeMillis()).nextInt(37000-1024);
        randomTimes += 1024;
        for (int i=0; i<randomTimes; i++) {
            outputStream.write(bytes);
        }
    }
}
