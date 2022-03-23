package org;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HdfsDu2 {


    public static void main(String[] args) {
        try {
//            double x = 111111.121212;
//            System.out.printf("%,10.2f \n", x);
//            System.exit(1);

            int ia = 0;
            String root = args[ia++];
            int numWorkers = Integer.valueOf(args[ia++]);
            String rootDir = args[ia++];
            String outputFileName = args[ia++];


            Configuration hdfsConfig = new Configuration();

//            hdfsConfig.addResource(new Path("file:///etc/hadoop/conf/core-site.xml")); // Replace with actual path
//            hdfsConfig.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml")); // Replace with actual path
//            hdfsConfig.addResource(new Path("file:///etc/hadoop/conf/kms-site.xml")); // Replace with actual path
//
//            hdfsConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            //org.apache.hadoop.hdfs.DistributedFileSystem
            //hdfsConfig.addResource(root);

            //hdfsConfig.set("fs.defaultFS", root);
            //hdfsConfig.set("fs.default.name", root);

            List<String> list = hdfsConfig.getValByRegex(".*").keySet().stream().sorted().collect(Collectors.toList());
            for (String e : list) {
                System.out.printf("HDFS conf: %s = %s\n", e, hdfsConfig.get(e));
            }
            final FileSystem fs = FileSystem.get(hdfsConfig);
            final BossWorkerQueue<FileStatus> queue = new BossWorkerQueue<FileStatus>(numWorkers);
            final AtomicLong countDir = new AtomicLong();
            final AtomicLong countFile = new AtomicLong();
            FileStatus first = fs.getFileStatus(new Path(rootDir));
            if (!first.isDirectory()) {
                System.err.printf("first path \"%s\" is not a directory\n", first.getPath());
                return;
            }

            queue.give(first);

            final ArrayBlockingQueue<Optional<String>> writeQueue = new ArrayBlockingQueue<Optional<String>>(100000);

            Thread writer = new Thread(() -> {

                try (FSDataOutputStream outfile = fs.create(new Path(outputFileName))) {

                    while (true) {
                        Optional<String> l = writeQueue.take();
                        if (l.isPresent())
                            outfile.writeBytes(l.get() + "\n");
                        else
                            return;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
            writer.setName("writer");
            writer.start();

            List<Thread> workers = IntStream.range(0, numWorkers).boxed().map(i -> {
                Thread t = new Thread(() -> {
                    try {
                        ArrayList<FileStatus> dirList = new ArrayList<FileStatus>(1000);
                        StringBuilder bld = new StringBuilder(100);
                        while (true) {
                            dirList.clear();

                            FileStatus dirStatus = queue.take();
                            if (dirStatus == null)
                                break;

//                            DirStats stat = new DirStats();
//                            stat.path = dirStatus.getPath();


                            FileStatus[] filelist = null;
                            try {
                                filelist = fs.listStatus(dirStatus.getPath());
                            } catch (Exception e) {
                                String s = e.getMessage();
                                int index = s.indexOf('\n');
                                if ( index > 0 )
                                    s = s.substring(0, index);
                                System.err.println("ex: " + s);
                                continue;
                            }
                            for (FileStatus entry : filelist) {
                                bld.setLength(0);
                                if (entry.isDirectory()) {
                                    dirList.add(entry);
                                    bld.append("D|");
                                    countDir.incrementAndGet();
//                                    stat.dircount++;
                                } else if (entry.isSymlink()) {
                                    bld.append("S|");
//                                    stat.size += entry.getLen();
//                                    stat.filecount++;
//                                    stat.oldest = Math.max(entry.getModificationTime(), stat.oldest);
//                                    stat.youngest = Math.min(entry.getModificationTime(), stat.youngest);
                                } else {
                                    countFile.incrementAndGet();
                                    bld.append("F|");
                                }
                                bld.append(entry.getPath())
                                        .append('|')
                                        .append(entry.getLen())
                                        .append('|')
                                        .append(entry.getModificationTime())
                                        .append('|')
                                        .append(entry.getOwner());
                                writeQueue.put(Optional.of(bld.toString()));
                            }
                            //walkUpTree(stat);
                            for (FileStatus entry : dirList) {
                                queue.give(entry);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                t.setName("work" + i);
                System.out.println("starting thread: " + i);
                t.start();
                return t;
            }).collect(Collectors.toList());

            Thread ticker = new Thread(() -> {
                long start = System.nanoTime();
                long lastTime = start;
                try {
                    long lastDir = 0L;
                    long lastFile = 0L;
                    while (true) {
                        Thread.sleep(5000);
                        long now = System.nanoTime();
                        double runtime = (start - now)/1000.0D;
                        long intervalTime = (now - lastTime);
                        long thisDirCount = countDir.get();
                        long thisFileCount = countFile.get();
                        long deltaDir = thisDirCount - lastDir;
                        long deltaFile = thisFileCount - lastFile;
                        double rateFile = deltaFile * 1_000_000_000.0 / intervalTime;
                        double avgFileRate = thisFileCount * 1_000_000_000.0 / (now - start);
                        System.out.printf("%7s q-size: %,10d dir: %,10d (%,8d) file: %,10d (%,8d) file rate: %,6.0f  Avg rate: %,6.0f\n",
                                humanTime(now - start, 2),
                                queue.size(),
                                thisDirCount, deltaDir,
                                thisFileCount, deltaFile, rateFile, avgFileRate);
                        lastDir = thisDirCount;
                        lastFile = thisFileCount;
                        lastTime = now;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            ticker.setDaemon(true);
            ticker.setName("tic");
            ticker.start();

            for (Thread t : workers) {
                t.join();
            }
            //ticker.interrupt();
            writeQueue.put(Optional.empty());
            writer.join();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void updateFileNotes(FileStatus entry) {
        System.out.println("F: " + entry.getPath().toString());
    }

    private static void updateDirNotes(FileStatus entry) {
        System.out.println("D: " + entry.getPath().toString());


    }

    public static class DirStats {
        public Path path;

        public long size = 0;
        public long dircount = 0;
        public long filecount = 0;
        public long oldest = Long.MIN_VALUE;
        public long youngest = Long.MAX_VALUE;

        public long recur_size = 0;
        public long recur_dircount = 0;
        public long recur_filecount = 0;
        public long recur_oldest = Long.MAX_VALUE;
        public long recur_youngest = Long.MIN_VALUE;


        public void mergeSubUpTree(DirStats stats) {
            this.recur_size += stats.size;
            this.recur_dircount += stats.dircount;
            this.recur_filecount += stats.filecount;
            this.recur_oldest = Math.min(this.recur_oldest, stats.oldest);
            this.recur_youngest = Math.max(this.recur_youngest, stats.youngest);
        }

        private final static char del = '|';

        @Override
        public String toString() {
            return path.toString() + del + this.size + del + this.filecount + del + this.dircount + del + this.oldest + del + this.youngest;
        }
    }

    private static final long micros = 1000L;
    private static final long millis = micros * 1000L;
    private static final long second = millis * 1000L;
    private static final long minute = second * 60L;
    private static final long hour = minute * 60L;
    private static final long day = hour * 24L;
    private static final long week = day * 7L;
    final static String[] timeUnit = new String[]{"w", "d", "h", "m", "s", "ms", "u", "ns"};

    public static String humanTime(long nanotime, int precision) {
        if (precision < 1)
            precision = 1;

        long[] parts = {
                nanotime / week,
                (nanotime % week) / day,
                (nanotime % day) / hour,
                (nanotime % hour) / minute,
                (nanotime % minute) / second,
                (nanotime % second) / millis,
                (nanotime % millis) / micros,
                (nanotime % micros),
        };

        StringBuilder b = new StringBuilder(12);
        for (int i = 0; i < parts.length; i++) {
            if (parts[i] > 0) {
                b.append(parts[i]).append(timeUnit[i]);
                precision--;
                if (precision <= 0)
                    break;
            }
        }

        return b.toString();
    }

    public static ConcurrentHashMap<Path, DirStats> data = new ConcurrentHashMap<>(10001);
}
