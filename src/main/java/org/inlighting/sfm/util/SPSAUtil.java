package org.inlighting.sfm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SPSAUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SPSAUtil.class);

    // 表示将来的
    private NowCursor nowCursor;

    private final double[] DELTA_ARRAY = new double[]{-1, 1};
    private final double MIN_READAHEAD_SIZE;
    private final double MAX_READAHEAD_SIZE;
    private final double START_READAHEAD_SIZE;

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

    // a,c = (max-min)/2
    public SPSAUtil(double minReadaheadSize, double maxReadaheadSize, double startReadaheadSize) {
        A=1;
//        a=(maxReadaheadSize-minReadaheadSize)/2;
        a=8;
        MIN_READAHEAD_SIZE = minReadaheadSize;
        MAX_READAHEAD_SIZE = maxReadaheadSize;
        START_READAHEAD_SIZE = startReadaheadSize;
        // first step size
        c = 5;
        x = START_READAHEAD_SIZE;
        nowCursor = NowCursor.left;
        LOG.info(String.format("Init SPSAComponent max:%f, min:%f, a:%f, c:%f, startSize:%f", MIN_READAHEAD_SIZE,
                MAX_READAHEAD_SIZE, a, c, START_READAHEAD_SIZE));
    }

    public double requestNextReadaheadSize(double lastTimeResult) {
        // found global minimal
        switch (nowCursor) {
            case left:
                return calXMinus();
            case right:
                return calXPlus(lastTimeResult);
            case none:
                calX(lastTimeResult);
                return requestNextReadaheadSize(lastTimeResult);
        }
        return 0;
    }

    public double requestLastReadaheadSize() {
        switch (nowCursor) {
            case left:
                return xPlus;
            case right:
                return xMinus;
            case none:
                return xPlus;
            default:
                LOG.error("This should not be happened!");
                return 0;
        }
    }

    private double calXMinus() {
        k+=1;
        ak = a / Math.pow(k+1.0+A, alpha);
        ck = c / Math.pow(k+1.0, gamma);
        delta = generateDelta();
        xMinus = project(x-ck*delta);
        nowCursor = NowCursor.right;
        return xMinus;
    }

    private double calXPlus(double xMinusResultTmp) {
        this.xMinusResult = xMinusResultTmp;
        xPlus = project(x+ck*delta);
        nowCursor = NowCursor.none;
        return xPlus;
    }

    private void calX(double xPlusResultTmp) {
        xPlusResult = xPlusResultTmp;
        grad = (xPlusResult-xMinusResult) / (2*ck) * delta;
        x = project(x-ak*grad);
        LOG.debug(String.format("Start %fth iteration, ak:%f, ck:%f, delta:%f, xPlus:%f, xPlusResult:%f, " +
                        "xMinus:%f, xMinusResult:%f, grad:%f, x:%f", k, ak, ck, delta, xPlus, xPlusResult,
                xMinus, xMinusResult, grad, x));
        nowCursor = NowCursor.left;
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
