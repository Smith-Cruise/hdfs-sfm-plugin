package org.inlighting.sfm.readahead.component;

import java.util.concurrent.locks.Lock;

public class ReadaheadLock {

    private boolean isLocked = false;
    public synchronized void lock() {
        while(isLocked){
            try {
                wait();
            } catch (InterruptedException ignored) {}
        }
        isLocked = true;
    }
    public synchronized void unlock(){
        isLocked = false;
        notify();
    }
}
