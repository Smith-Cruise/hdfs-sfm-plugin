package org.inlighting.sfm.readahead.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticComponent implements ReadaheadComponent{

    private static final Logger LOG = LoggerFactory.getLogger(StaticComponent.class);

    private double staticReadaheadSize;

    @Override
    public void initialize(double minReadaheadSize, double maxReadaheadSize, double startReadaheadSize) {
        LOG.debug(String.format("Init static component. Static readahead size is %f", startReadaheadSize));
        staticReadaheadSize = startReadaheadSize;
    }

    @Override
    public void reInitialize() {

    }

    @Override
    public int requestNextReadaheadSize(double lastReadaheadHitRate) {
        return mb2Bytes(staticReadaheadSize);
    }

    @Override
    public int requestLastReadaheadSize() {
        return requestNextReadaheadSize(0);
    }


    private int mb2Bytes(double mb) {
        return (int) Math.round(mb*1024*1024);
    }
}
