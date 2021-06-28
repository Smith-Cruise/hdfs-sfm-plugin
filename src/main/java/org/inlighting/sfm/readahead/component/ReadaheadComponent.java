package org.inlighting.sfm.readahead.component;

// start with mb
public interface ReadaheadComponent {

    void initialize(double minReadaheadSize, double maxReadaheadSize, double startReadaheadSize);

    void reInitialize();

    int requestNextReadaheadSize(double lastReadaheadHitRate);

    int requestLastReadaheadSize();
}
