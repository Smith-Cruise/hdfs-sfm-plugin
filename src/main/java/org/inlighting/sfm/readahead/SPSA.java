package org.inlighting.sfm.readahead;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Scanner;

public class SPSA {

    private static final Logger LOG = LoggerFactory.getLogger(SPSA.class);

    private final double[] DELTA_ARRAY = new double[]{-1, 1};
    private final double MIN_READAHEAD_SIZE;
    private final double MAX_READAHEAD_SIZE;

    private final double A = 1;
    private double k = 0;
    private final double a;
    private final double c;
    private final double alpha = 0.602;
    private final double gamma = 0.101;
    private double x;

    boolean isFeed = true;

    public SPSA(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize) {
        MIN_READAHEAD_SIZE = minReadaheadSize;
        MAX_READAHEAD_SIZE = maxReadaheadSize;
        a = (double) (minReadaheadSize + maxReadaheadSize) / 2;
        c = 10;
        x = startReadaheadSize;
        LOG.info(String.format("Init SPSA max:%f, min:%f, a:%f, c:%f, start:%f", MIN_READAHEAD_SIZE,
                MAX_READAHEAD_SIZE, a, c, x));
    }

    public int recommendReadaheadSize() {
        if (isFeed) {
            k+=1;
            double ak = a / Math.pow(k+1.0+A, alpha);
            double ck = c / Math.pow(k+1, gamma);
            double delta = generateDelta();
            double xPlus = project(x+ck*delta);
            double xMinus = project(x-ck*delta);
            double grad = (enter(xPlus) - enter(xMinus)) / (2 * ck * delta);
            x = project(x - ak * grad);
            LOG.info(String.format("Start %fth iteration, ak:%f, ck:%f, delta:%f, xPlus:%f," +
                    "xMinus:%f, grad:%f, resultX:%f", k, ak, ck, delta, xPlus, xMinus,
                    grad, x));
            return (int)Math.round(x);
        } else {
            LOG.error("SPSA didn't feed xMinus, xPlus.");
            return -1;
        }
    }

    public double enter(double x) {
//        System.out.println("please enter "+ x);
//        return new Scanner(System.in).nextDouble();
        return Math.pow(x-30, 2) + 100;
    }

    public double project(double x) {
        x = Math.min(x, MAX_READAHEAD_SIZE);
        return Math.max(x, MIN_READAHEAD_SIZE);
    }

    public double generateDelta() {
        Random random = new Random(System.nanoTime());
        return DELTA_ARRAY[random.nextInt(2)];
    }
}
