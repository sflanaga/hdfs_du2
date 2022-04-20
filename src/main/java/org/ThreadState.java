package org;

import java.util.concurrent.locks.ReentrantLock;

public class ThreadState {



    final char[] states;
    // lock per entry?
    final ReentrantLock lock = new ReentrantLock();
    public ThreadState(int num) {
        states = new char[num];
        for (int i = 0; i < num; i++) {
            states[i] = '?';
        }
    }
    public void setState(int i, char c) {
        lock.lock();
        try {
            states[i] = c;
        } finally {
            lock.unlock();
        }
    }
    public String toString() {
        lock.lock();
        try {
            return new String(states);
        } finally {
            lock.unlock();
        }
    }
    // this is not meant to be super thread sync'ed precise and
    // setting this memory location... should be isolated much of the time.
    public void setStateLockless(int i, char c) {
        states[i] = c;
    }
}
