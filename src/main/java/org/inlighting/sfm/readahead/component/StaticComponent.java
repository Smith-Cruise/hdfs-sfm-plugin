package org.inlighting.sfm.readahead.component;

public class StaticComponent implements ReadaheadComponent{

    private int staticReadaheadSize;

    @Override
    public void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize) {
        staticReadaheadSize = startReadaheadSize;
    }

    @Override
    public void reInitialize() {

    }

    @Override
    public int requestNextReadaheadSize(double lastReadaheadHitRate) {
        return staticReadaheadSize;
    }

    @Override
    public int requestLastReadaheadSize() {
        return staticReadaheadSize;
    }
}
