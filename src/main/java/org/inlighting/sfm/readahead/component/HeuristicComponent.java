package org.inlighting.sfm.readahead.component;

public class HeuristicComponent implements ReadaheadComponent{

    private double MIN_READAHEAD_SIZE;
    private double MAX_READAHEAD_SIZE;
    private double START_READAHEAD_SIZE;
    private double x;


    @Override
    public void initialize(double minReadaheadSize, double maxReadaheadSize, double startReadaheadSize) {
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
        return mb2Bytes(x);
    }


    @Override
    public int requestLastReadaheadSize() {
        return 0;
    }

    private int mb2Bytes(double mb) {
        return (int) Math.round(mb*1024*1024);
    }
}
