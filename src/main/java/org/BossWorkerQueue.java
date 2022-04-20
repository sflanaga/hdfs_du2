package org;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BossWorkerQueue<T> {
    private final int numWorkers;
    public BossWorkerQueue(int numWorkers) {
        this.numWorkers = numWorkers;
        this.done = false;
    }

    final Lock lock = new ReentrantLock();
    private Condition stateChange = lock.newCondition();
    private boolean done;
    private int takers = 0;
    private int awaiters = 0;

    private LinkedList<T> queue = new LinkedList<>();

    public T take() throws InterruptedException { // return null to indicate done
        lock.lock();
        takers++;
        try {
            while(!done) {
                T j = queue.pollFirst();
                if (j != null) {
                    return j;
                } else {
                    if ( takers >= numWorkers || done ) {
                        System.out.println("signal all done");
                        done = true;
                        stateChange.signalAll();
                        return null;
                        // if we get here - we are done - there is not more work and no body has more work.
                    } else {
                        awaiters++;
                        stateChange.await(100, TimeUnit.MILLISECONDS);
                        awaiters--;
                    }
                }
            }
            return null;

        }finally {
            lock.unlock();
            takers--;
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }

    }
    public int getNumTakers() {
        lock.lock();
        try {
            return takers;
        } finally {
            lock.unlock();
        }
    }

    public void giveAll(List<T> jobList) {
        lock.lock();
        try {
            for(T j: jobList)
                queue.addFirst(j);
            if ( awaiters > 0 )
                stateChange.signalAll();
        } finally {
            lock.unlock();
        }

    }

    public void give(T job) {
        lock.lock();
        try {
            queue.addFirst(job);
            if ( awaiters > 0 )
                stateChange.signalAll();
        } finally {
            lock.unlock();
        }

    }

    public void giveLast(T job) {
        lock.lock();
        try {
            queue.addLast(job);
            if ( awaiters > 0 )
                stateChange.signalAll();
        } finally {
            lock.unlock();
        }

    }
    public void forceDone() {
        lock.lock();
        try {
            done = true;
            stateChange.signalAll();
        } finally {
            lock.unlock();
        }

    }

    public static AtomicLong count = new AtomicLong(0);

    public static void main(String[] args) {
        try {
            int numWorkers = 10;
            final BossWorkerQueue<Integer> q = new BossWorkerQueue<>(numWorkers);
            List<Thread> tl = IntStream.range(0,numWorkers).boxed().map(i -> {
               Thread t = new Thread(() -> {

                   try {
                       Random rand = new Random(503);

                       boolean giver = true;
                       while(true) {
                           if (giver) {
                               q.give(5);
                               count.incrementAndGet();
                               int x = rand.nextInt(5_000_000);
                               if (x == 667) {
                                   giver = false;
                                   System.out.println("become taker");
                               }

                           } else {
                               Integer j = q.take();
                               if ( j == null ) {
                                   return;
                               }
                           }
                       }
                   } catch(Exception e) {
                       e.printStackTrace();
                   }

                   System.out.println(i);
               });
               t.setName("work" + i);
                System.out.println("starting thread: " + i);
               t.start();
               return t;
            }).collect(Collectors.toList());

            Thread ticker = new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        System.out.printf("sz: %10d  cnt: %10d  takers: %10d\n", q.size(), count.get(), q.getNumTakers());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            ticker.setName("tick");
            ticker.setDaemon(true);
            ticker.start();


            for(Thread t: tl) {
                t.join();
            }
            System.out.println("DONE counted: " + count.get());

        }catch (Exception e) {
            e.printStackTrace();
        }
    }


}
