package org.inlighting.sfm.readahead;

import java.io.IOException;

public interface ReadaheadManager {

    int readFully(long position, byte[] b, int off, int len) throws IOException;

    ReadaheadEntity readahead(long startPosition, int size) throws IOException;
}
