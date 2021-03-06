package org.inlighting.sfm.readahead.component;

// start with mb
public interface ReadaheadComponent {

    void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize);

    void reInitialize();

    int requestNextReadaheadSize(double lastReadaheadHitRate);

}
