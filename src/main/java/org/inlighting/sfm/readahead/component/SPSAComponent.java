package org.inlighting.sfm.readahead.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SPSAComponent implements ReadaheadComponent {

    private static final Logger LOG = LoggerFactory.getLogger(SPSAComponent.class);

    // 表示将来的
    private NowCursor nowCursor;

    private final double[] DELTA_ARRAY = new double[]{-1, 1};
    private double MIN_READAHEAD_SIZE;
    private double MAX_READAHEAD_SIZE;
    private double START_READAHEAD_SIZE;

    private double A;
    private double a;
    private double c;
    private final double alpha = 0.602;
    private final double gamma = 0.101;

    // global variable
    private double k = 0;
    private double ak;
    private double ck;
    private double delta;
    private double xPlus;
    private double xPlusResult;
    private double xMinus;
    private double xMinusResult;
    private double grad;
    private double x;

    @Override
    public void initialize(int minReadaheadSize, int maxReadaheadSize, int startReadaheadSize) {
        A=1;
        a=10;
        MIN_READAHEAD_SIZE = minReadaheadSize;
        MAX_READAHEAD_SIZE = maxReadaheadSize;
        START_READAHEAD_SIZE = startReadaheadSize;
        // recommend std
        c =(double) (maxReadaheadSize - minReadaheadSize) / 2;
        x = startReadaheadSize;
        nowCursor = NowCursor.left;
        LOG.info(String.format("Init SPSAComponent max:%fMB, min:%fMB, a:%f, c:%f, startSize:%fMB", MIN_READAHEAD_SIZE,
                MAX_READAHEAD_SIZE, a, c, x));
    }

    @Override
    public void reInitialize() {
        nowCursor = NowCursor.left;
        x = START_READAHEAD_SIZE;
        k=0;
        LOG.info("Reinitialize SPSAComponent");
    }

    @Override
    public int requestNextReadaheadSize() {
        return (int) Math.round(START_READAHEAD_SIZE);
    }

    @Override
    public int requestNextReadaheadSize(double lastTimeResult) {
        // found global minimal
        lastTimeResult = -lastTimeResult;
        switch (nowCursor) {
            case left:
                LOG.debug("SPSA left, don't need lastTimeResult.");
                k+=1;
                ak = a / Math.pow(k+1.0+A, alpha);
                ck = c / Math.pow(k+1, gamma);
                delta = generateDelta();
                xPlus = project(x+ck*delta);
                nowCursor = NowCursor.right;
                return (int) Math.round(xPlus);
            case right:
                xPlusResult = lastTimeResult;
                LOG.debug("SPSA right, xPlusResult: "+xPlusResult);
                xMinus = project(x-ck*delta);
                nowCursor = NowCursor.none;
                return (int) Math.round(xMinus);
            case none:
                xMinusResult = lastTimeResult;
                LOG.debug("SPSA none, xMinusResult: "+xMinusResult);
                grad = (xPlusResult-xMinusResult) / (2*ck*delta);
                x = project(x-ak*grad);
                LOG.info(String.format("Start %fth iteration, ak:%f, ck:%f, delta:%f, xPlus:%f, xPlusResult:%f, " +
                                "xMinus:%f, xMinusResult:%f, grad:%f, resultX:%f", k, ak, ck, delta, xPlus, xPlusResult,
                        xMinus, xMinusResult, grad, x));
                nowCursor = NowCursor.left;
                // do not return result, jump to next iteration.
                // return (int) Math.round(x);
                return requestNextReadaheadSize(lastTimeResult);
        }
        return 0;
    }

    private double project(double x) {
        x = Math.min(x, MAX_READAHEAD_SIZE);
        return Math.max(x, MIN_READAHEAD_SIZE);
    }

    private double generateDelta() {
        Random random = new Random(System.nanoTime());
        return DELTA_ARRAY[random.nextInt(2)];
    }

    private enum NowCursor {
        left, right, none;
    }
}
