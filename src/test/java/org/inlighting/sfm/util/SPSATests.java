package org.inlighting.sfm.util;

import org.inlighting.sfm.readahead.component.SPSAComponent;
import org.junit.jupiter.api.Test;


public class SPSATests {

    @Test
    void testSPSA() {
        SPSAComponent spsa = new SPSAComponent();
        spsa.initialize(10, 50, 10);
        double lastResult = func(10);
        for (int i=0; i<2000; i++) {
            lastResult = func(spsa.requestNextReadaheadSize(lastResult));
            System.out.println(lastResult);
        }

    }

    private double func(int x) {
        return (x-30)*(x-30) + 10;
    }
}
