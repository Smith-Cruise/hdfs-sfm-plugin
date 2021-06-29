package org.inlighting.sfm.util;

import org.junit.jupiter.api.Test;


public class SPSAUtilTests {

    @Test
    void testSPSA() {
        SPSAUtil spsa = new SPSAUtil(1, 10, 5);
        double firstNum = spsa.requestNextReadaheadSize(111);
        double lastResult = func(firstNum);
        for (int i=0; i<2000; i++) {
            double num = spsa.requestNextReadaheadSize(lastResult);
            lastResult = func(num);
        }

    }

    private double func(double x) {
        return (x-7)*(x-7) + 10;
    }
}
