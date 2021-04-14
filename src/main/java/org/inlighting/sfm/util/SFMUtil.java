package org.inlighting.sfm.util;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
* 除了专门检测的会校验 sfm uri 是否合法，别的都不会校验
* */
public class SFMUtil {

    // Only check path contains .sfm
    public static boolean isValidSFMPath(URI uri) {
        String regex = "(?<=/).+.sfm";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(uri.getPath());
        return m.find();
    }

    public static String getFilename(URI uri) throws IOException {
        String path = uri.getPath();
        if (path.endsWith(".sfm") || path.endsWith(".sfm/")) {
            return null;
        }
        String regex = "(?<=.sfm/).+";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(path);
        if (m.find()) {
            return m.group();
        } else {
            throw new IOException("Illegal SFM path.");
        }
    }

    public static String getSFMBasePath(URI uri) throws IOException {
        String path = uri.getPath();
        String regex = "/.+.sfm";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(path);
        if (! m.find()) {
            throw new IOException("Cannot get SFM name path.");
        }
        return m.group();
    }

    public static byte getUnsignedByte (int a) {
        if (a < 0 || a > 255) {
            throw new IllegalArgumentException("Invalid number. Range 0<=x<=255.");
        }
        return (byte) a;
    }

    public static int readUnsignedByte(byte a) {
        return a & 0x0FF;
    }

}
