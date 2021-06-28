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
    public void initialize(double minReadaheadSize, double maxReadaheadSize, double startReadaheadSize) {
        A=1;
        a=10;
        MIN_READAHEAD_SIZE = minReadaheadSize;
        MAX_READAHEAD_SIZE = maxReadaheadSize;
        START_READAHEAD_SIZE = startReadaheadSize;
        // recommend std
        c = (maxReadaheadSize - minReadaheadSize) / 2;
        x = startReadaheadSize;
        nowCursor = NowCursor.left;
        LOG.info(String.format("Init SPSAComponent max:%fMB, min:%fMB, a:%f, c:%f, startSize:%fMB", MIN_READAHEAD_SIZE,
                MAX_READAHEAD_SIZE, a, c, x));
    }

    @Override
    public void reInitialize() {
//        nowCursor = NowCursor.left;
//        x = START_READAHEAD_SIZE;
//        k=0;
//        LOG.info("Reinitialize SPSAComponent");
    }


    @Override
    public int requestNextReadaheadSize(double lastTimeResult) {
        // found global minimal
        switch (nowCursor) {
            case left:
                return mb2Bytes(calXMinus());
            case right:
                return mb2Bytes(calXPlus(lastTimeResult));
            case none:
                calX(lastTimeResult);
                return requestNextReadaheadSize(lastTimeResult);
        }
        return 0;
    }

    @Override
    public int requestLastReadaheadSize() {
        switch (nowCursor) {
            case left:
                return mb2Bytes(xPlus);
            case right:
                return mb2Bytes(xMinus);
            case none:
                return mb2Bytes(xPlus);
            default:
                LOG.error("This should not be happened!");
                return 0;
        }
    }

    private double calXMinus() {
        LOG.debug("SPSA left, don't need lastTimeResult.");
        k+=1;
        ak = a / Math.pow(k+1.0+A, alpha);
        ck = c / Math.pow(k+1, gamma);
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
        grad = (xPlusResult-xMinusResult) / (2*ck*delta);
        x = project(x-ak*grad);
        LOG.debug(String.format("Start %fth iteration, ak:%f, ck:%f, delta:%f, xPlus:%f, xPlusResult:%f, " +
                        "xMinus:%f, xMinusResult:%f, grad:%f, resultX:%f", k, ak, ck, delta, xPlus, xPlusResult,
                xMinus, xMinusResult, grad, x));
        nowCursor = NowCursor.left;
    }

    private int mb2Bytes(double mb) {
        return (int) Math.round(mb*1024*1024);
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
