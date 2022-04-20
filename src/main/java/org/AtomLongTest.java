package org;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AtomLongTest {

    public static void main(String[] args) {
        for (int numThreads = 0; numThreads < 8; numThreads++) {
            try {

                final AtomicLong count = new AtomicLong();
                final long iterations = 100_000_000;

                long start = System.currentTimeMillis();
                List<Thread> workers = IntStream.range(0, numThreads).boxed().map(i -> {
                    Thread t = new Thread(() -> {
                        for (long j = 0; j < iterations; j++) {
                            count.incrementAndGet();
                        }
                    });
                    t.start();
                    return t;
                }).collect(Collectors.toList());

                for (Thread t : workers) {
                    t.join();
                }
                long end = System.currentTimeMillis();
                System.out.printf("threads: %d runtime: %d ms  iteration: %,d count: %,d  rate: %,d count / sec\n", numThreads, (end - start), iterations, count.get(), count.get() * 1000 / (end - start));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
