package org.inlighting.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SFMUtil {

    public static void checkValidSFM(String path) throws IOException {
        Path tmp = new Path(path);
        if (! tmp.isAbsolute()) {
            throw new IOException("SFM path must be an absolute path.");
        }
        String regex = "(?<=/).+.sfm";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(path);
        if (! m.find()) {
            throw new IOException("Illegal SFM path.");
        }
    }

    public static void checkValidSFM(Path path) throws IOException {
        checkValidSFM(path.toUri().getPath());
    }

    public static void checkValidSFM(URI uri) throws IOException {
        checkValidSFM(uri.getPath());
    }

    public static String getFilename(String path) throws IOException {
        checkValidSFM(path);
        String regex = "(?<=.sfm/).+";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(path);
        if (m.find()) {
            return m.group();
        } else {
            throw new IOException("Illegal SFM path.");
        }
    }

    public static String getFilename(URI uri) throws IOException {
        return getFilename(uri.getPath());
    }

//    public static String getSFMName(String path) throws IOException {
//        checkValidSFM(path);
//        String regex = "/.+.sfm";
//        Pattern p = Pattern.compile(regex);
//        Matcher m = p.matcher(path);
//        if (! m.find()) {
//            throw new IOException("Cannot get SFM name.");
//        }
//        String tmp = m.group();
//        return StringUtils.substringAfterLast(tmp, "/");
//    }
//
//    public static String getSFMName(URI uri) throws IOException {
//        return getSFMName(uri.getPath());
//    }

    public static String getSFMBasePath(String path) throws IOException {
        checkValidSFM(path);
        String regex = "/.+.sfm";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(path);
        if (! m.find()) {
            throw new IOException("Cannot get SFM name path.");
        }
        return m.group();
    }

    public static String getSFMBasePath(URI uri) throws IOException {
        return getSFMBasePath(uri.getPath());
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
