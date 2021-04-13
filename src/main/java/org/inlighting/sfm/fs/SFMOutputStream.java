package org.inlighting.sfm.fs;


import org.inlighting.sfm.SFMConstants;
import org.inlighting.sfm.merger.FileEntity;
import org.inlighting.sfm.merger.SFMerger;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

public class SFMOutputStream extends OutputStream {

    private final SFMerger SFMERGER;

    private final String TMP_FOLDER = SFMConstants.SFMERGER_TMP_FOLDER;

    private final BufferedOutputStream OUT;

    private final UUID UUID = java.util.UUID.randomUUID();

    private boolean closed = false;

    private byte[] singleByte = new byte[1];

    private long writtenLength = 0L;

    private final FileEntity FILE_ENTITY = new FileEntity();

    public SFMOutputStream(SFMerger sfMerger, String sfmBasePath, String fileName) throws IOException {
        SFMERGER = sfMerger;

        // create a local file
        OUT = new BufferedOutputStream(new FileOutputStream(TMP_FOLDER + "/" + getTmpFilename()));

        FILE_ENTITY.setTombstone(false);
        FILE_ENTITY.setSfmBasePath(sfmBasePath);
        FILE_ENTITY.setFilename(fileName);
        FILE_ENTITY.setTmpStoreName(getTmpFilename());
    }

    public String getTmpFilename() {
        return UUID.toString();
    }

    private synchronized long getWrittenLength() throws IOException {
        return writtenLength;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("SFMOutputStream is closed.");
        }
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        checkClosed();
        OUT.write(b, off, len);
        writtenLength += len;
    }

    @Override
    public synchronized void flush() throws IOException {
        OUT.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        OUT.close();

        FILE_ENTITY.setFilesSize(getWrittenLength());
        SFMERGER.add(FILE_ENTITY);
        closed = true;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        singleByte[0] = (byte) b;
        write(singleByte, 0, 1);
    }
}
