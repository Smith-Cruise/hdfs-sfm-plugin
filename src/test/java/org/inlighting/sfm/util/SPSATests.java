package org.inlighting.sfm.util;

import org.inlighting.sfm.readahead.SPSA;
import org.junit.jupiter.api.Test;


public class SPSATests {

    @Test
    void testSPSA() {
        SPSA spsa = new SPSA(10, 50, 10);
        for (int i=0; i<10; i++) {
            System.out.println(spsa.recommendReadaheadSize());
        }

    }
}
