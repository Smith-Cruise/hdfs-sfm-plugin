package org.inlighting.sfm.readahead.component;

public class HeuristicComponent implements ReadaheadComponent{

    private int MIN_READAHEAD_SIZE;
    private int MAX_READAHEAD_SIZE;
    private int START_READAHEAD_SIZE;
    private int x;


    @Override
    public void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize) {
        MIN_READAHEAD_SIZE = minReadaheadSize;
        MAX_READAHEAD_SIZE = maxReadaheadSize;
        START_READAHEAD_SIZE = startReadaheadSize;
        x = startReadaheadSize;
    }

    @Override
    public void reInitialize() {
        x = START_READAHEAD_SIZE;
    }

    @Override
    public int requestNextReadaheadSize(double lastReadaheadHitRate) {
        // 暂时不做判断，一直扩大size，直到最大
        x = Math.max(MIN_READAHEAD_SIZE, x);
        x = Math.min(MAX_READAHEAD_SIZE, x);
        return x;
    }

    @Override
    public int requestNextReadaheadSize() {
        return START_READAHEAD_SIZE;
    }
}
