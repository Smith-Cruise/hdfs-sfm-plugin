package org.inlighting.sfm;

import org.inlighting.sfm.readahead.ReadaheadManagerEnum;

public class SFMConstants {

    public static final String INDEX_NAME = "_index";

    public static final String MASTER_INDEX_NAME = "_masterIndex";

    public static final String MERGED_FILENAME = "part-0";

    public static final String SFMERGER_TMP_FOLDER = "TMP_SFM";


    public static final boolean OVERWRITE_TMP_FOLDER = true;

    public static final String DEFAULT_UNDERLYING_FS = "hdfs";

    public static final int TRAILER_INDEX_VERSION = 0;

    public static final int MAX_NUM_INDEX = 100000;

    // readahead
    public static final boolean ENABLE_CACHE = true;
    public static final int READAHEAD_CACHE_NUM = 10;
    public static final ReadaheadManagerEnum READAHEAD_MANAGER_ENUM = ReadaheadManagerEnum.spsa;
}
