package org.inlighting.sfm.readahead.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticComponent implements ReadaheadComponent{

    private static final Logger LOG = LoggerFactory.getLogger(StaticComponent.class);

    private int staticReadaheadSize;

    @Override
    public void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize) {
        LOG.debug(String.format("Init static component. Static readahead size is %d", startReadaheadSize));
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
