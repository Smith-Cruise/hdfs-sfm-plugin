package org.inlighting.sfm.readahead.component;

public interface ReadaheadComponent {

    void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize);

    void reInitialize();

    int requestNextReadaheadSize(double lastReadaheadHitRate);

    // first time request
    int requestNextReadaheadSize();

}
