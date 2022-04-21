package org;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HdfsDu2 {
    private static boolean debug = false;

    public static class Cli {
        @CommandLine.Option(names = {"-t", "--thread_workers"}, arity = "1", defaultValue = "4",
                description = "number of workers thread listing directories")
        int numWorkers;

        @CommandLine.Option( names = {"-z", "--zstd_output"},
                description = "enable zstd output - a .zst will be adding to output filename as well")
        boolean zstdOutput;

        @CommandLine.Option( names = {"-c", "--zstd_compression_level"}, required = false, defaultValue = "3",
                description = "sets zstd compression level - 3 is default")
        int zstdCompressionLevel;

        @CommandLine.Option( names = {"-C", "--config_properties"}, required = false, arity = "1..N",
                description = "load configuration from 1 or more configuration files as hadoop resources")
        List<java.nio.file.Path> confProps;

        @CommandLine.Option( names = {"-q", "--write_queue_size"}, required = false, defaultValue = "100000",
                description = "size of the queue between the workers and the 1 writer thread")
        int writeQueueSize;

        @CommandLine.Parameters(index="0", description = "top or root path to descend into")
        String rootDir;

        @CommandLine.Option(names = {"-s", "--single_directory"}, defaultValue = "false",
                description = "get a single directory of files/paths - default is to recurse")
        boolean singleDir;

        @CommandLine.Option(names = {"-l", "--write_local"},
                description = "write output to local file system instead of the default hdfs")
        boolean writeLocalOutput;

        @CommandLine.Option(names = {"-p", "--print_properties"},
                description = "print effective hdfs properties - for debugging")
        boolean printProperties;

        @CommandLine.Parameters(index="1", description = "output file/path in hdfs")
        String outputFileName;

        @CommandLine.Option( names = {"-L", "--limit_output"}, required = false, defaultValue = "0",
                description = "limits output to N records or some above - not exacting - 0 is default and means no limit")
        long limit;

        @CommandLine.Option( names = {"-d", "--debug"}, required = false,
                description = "writes lots of extra debug output to stderr")
        boolean debug;

        @CommandLine.Option( names = {"--core_site"}, defaultValue = "/etc/hadoop/conf/core-site.xml",
                description = "core-site xml configuration to use")
        String coreSitePath;

        @CommandLine.Option( names = {"--hdfs_site"}, defaultValue = "/etc/hadoop/conf/hdfs-site.xml",
                description = "hdfs-site xml configuration to use")
        String hdfsSitePath;

        @CommandLine.Option( names = {"--krb5_conf"}, defaultValue = "/etc/krb5.conf",
                description = "krb5 main conf location")
        String krb5Path;

        @CommandLine.Option( names = {"--krb5_user"}, required=true,
                description = "krb5 user - e.g. adm_joe1@HDPQUANTUMPROD.COM")
        String krbUser;

        @CommandLine.Option( names = {"--krb5_key_tab"}, required=true,
                description = "krb5 key tab file location - e.g. /etc/security/keytabs/adm_joe1.user.keytab")
        String krbKeyTab;

        @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message\nsample cmdline: java -cp hdfs_du2-1.0-SNAPSHOT.jar:lib/* org.HdfsDu2 /prod test --krb5_user adm_sflanag1@HDPQUANTUMPROD.COM --krb5_key_tab /etc/security/keytabs/adm_sflanag1.user.keytab")
        boolean usageHelpRequested;
    }

    public static BufferedOutputStream createOutputStream(Cli cli, FileSystem fs) throws IOException {
        String outfilename = cli.outputFileName;
        OutputStream os = null;
        if ( cli.zstdOutput ) {
            if (!outfilename.endsWith(".zst")) {
                outfilename += ".zst";
                System.err.println("Modifying output filename to (compressed zst)" + outfilename);
            }
        }

        // local or hdfs
        if ( cli.writeLocalOutput ) {
            java.nio.file.Path path = Paths.get(outfilename);
            System.err.println("writing local: " + path.toFile().getAbsolutePath());
            os = Files.newOutputStream(path);
        } else {
            os = fs.create(new Path(outfilename));
        }

        // compressed with zstd or not
        if ( cli.zstdOutput ) {
            return new BufferedOutputStream(
                new ZstdCompressorOutputStream(os, cli.zstdCompressionLevel));
        } else {
            return new BufferedOutputStream(os);
        }

    }

    public static void populateConfigWithProperties(Configuration cfg, List<java.nio.file.Path> propFileList) throws IOException {
        for ( java.nio.file.Path path: propFileList) {
            if ( debug )
                System.err.println("reading configuration from: " + path);

            cfg.addResource(path.toString());
        }
        /*
        Properties prop = new Properties();
        InputStream is =Files.newInputStream(propFile);
        if ( propFile.toString().contains(".xml"))
            prop.loadFromXML(is);
        else
            prop.load(is);
        for(Map.Entry<Object, Object> p: prop.entrySet()) {
            cfg.set(p.getKey().toString(), p.getValue().toString());
        }
        cfg.addResource();

         */
    }


    public static void main(String[] args) {
        try {

            final Cli cli = new Cli();
            try {
                CommandLine cl = new CommandLine(cli);
                cl.parseArgs(args);
                if (cli.usageHelpRequested) {
                    cl.usage(System.err);
                    return;
                }
                debug = cli.debug;
            } catch (Exception e) {
                System.err.println("cli related exception: " + e);
                return;
            }

//            Configuration hdfsConfig = new Configuration();
//            if ( cli.confProps != null )
//                populateConfigWithProperties(hdfsConfig, cli.confProps);
//
//            if ( cli.debug ) {
//                for(String k: hdfsConfig.getValByRegex(".*").keySet().stream().sorted().collect(Collectors.toList()))
//                    System.err.printf("hdfs prop: %s = %s\n", k, hdfsConfig.get(k));
//            }
//
//            if ( cli.printProperties) {
//                List<String> list = hdfsConfig.getValByRegex(".*").keySet().stream().sorted().collect(Collectors.toList());
//                for (String e : list) {
//                    System.out.printf("HDFS conf: %s = %s\n", e, hdfsConfig.get(e));
//                }
//            }

            if ( !Files.exists(Paths.get(cli.coreSitePath)))
                throw new RuntimeException("core site file: " + cli.coreSitePath + " does not exist");
            if ( !Files.exists(Paths.get(cli.hdfsSitePath)))
                throw new RuntimeException("hdfs site file: " + cli.hdfsSitePath + " does not exist");

            final FileSystem fs = Util.getHdfsFileSystem(cli.debug, cli.krbUser, cli.coreSitePath, cli.hdfsSitePath, cli.krb5Path, cli.krbKeyTab);
            //new DistributedFileSystem(hdfsConfig); // FileSystem.get(hdfsConfig);
            if ( debug )
                System.err.println("FileSystem type: " + fs.getClass().getName());
            final BossWorkerQueue<FileStatus> queue = new BossWorkerQueue<FileStatus>(cli.numWorkers);
            final AtomicLong countDir = new AtomicLong();
            final AtomicLong countFile = new AtomicLong();
            final AtomicLong countBytes = new AtomicLong();
            final AtomicLong recordsWritten = new AtomicLong();
            FileStatus first = fs.getFileStatus(new Path(cli.rootDir));
            if (!first.isDirectory()) {
                System.err.printf("first path \"%s\" is not a directory\n", first.getPath());
                return;
            }

            if ( cli.singleDir ) {
                FileStatus[] filelist = fs.listStatus(first.getPath());
                for(FileStatus sta: filelist) {
                    System.out.println(sta.getPath());
                }
            } else { // recurse the world

                queue.give(first);

                final ArrayBlockingQueue<Optional<byte[]>> writeQueue = new ArrayBlockingQueue<Optional<byte[]>>(cli.writeQueueSize);

                final ThreadState threadState = new ThreadState(cli.numWorkers + 1);

                Thread writer = new Thread(() -> {
                    int pos = cli.numWorkers;
                    try (BufferedOutputStream outfile = createOutputStream(cli, fs)) {
                        while (true) {
                            threadState.setStateLockless(pos, 'Q');
                            Optional<byte[]> l = writeQueue.take();
                            threadState.setStateLockless(pos, 'W');
                            if (l.isPresent()) {
                                outfile.write(l.get());
                                countBytes.addAndGet(l.get().length);
                                recordsWritten.incrementAndGet();
                                // we cannot allow this guy to quit or it may limit writers ability to finish.
                                // so will usually go past the limit
                            } else
                                return;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        threadState.setStateLockless(pos, '*');
                    }

                });
                writer.setName("writer");
                writer.start();

                List<Thread> workers = IntStream.range(0, cli.numWorkers).boxed().map(i -> {
                    Thread t = new Thread(() -> {
                        try {
                            threadState.setStateLockless(i, 'S');
                            ArrayList<FileStatus> dirList = new ArrayList<FileStatus>(1000);
                            StringBuilder bld = new StringBuilder(100);
                            while (true) {
                                dirList.clear();

                                threadState.setStateLockless(i, 'Q');
                                FileStatus dirStatus = queue.take();
                                threadState.setStateLockless(i, 'L');
                                if (dirStatus == null)
                                    break;

                                FileStatus[] filelist = null;
                                try {
                                    filelist = fs.listStatus(dirStatus.getPath());
                                } catch (Exception e) {
                                    String s = e.getMessage();
                                    int index = s.indexOf('\n');
                                    if (index > 0)
                                        s = s.substring(0, index);
                                    System.err.println("ex: " + s);
                                    continue;
                                }
                                threadState.setStateLockless(i, 'P');

                                long puts = 0;
                                for (FileStatus entry : filelist) {
                                    bld.setLength(0);
                                    if (entry.isDirectory()) {
                                        dirList.add(entry);
                                        bld.append("D|");
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
                                    // trying to offload UTF 8 encoding in worker thread
                                    writeQueue.put(Optional.of((bld.toString() + "\n").getBytes(StandardCharsets.UTF_8)));
                                    puts++;
                                }
                                countFile.addAndGet(puts);

                                threadState.setStateLockless(i, 'R');

                                queue.giveAll(dirList);
                                countDir.addAndGet(dirList.size());

                                if (cli.limit > 0)
                                    if (cli.limit <= (recordsWritten.get())) {
                                        System.err.printf("Early finish as write limit %d reach at %d\n", cli.limit, recordsWritten.get());
                                        queue.forceDone();
                                        threadState.setStateLockless(i, 'F');

                                    }

                            }
                            threadState.setStateLockless(i, 'D');

                        } catch (Exception e) {
                            e.printStackTrace();
                            threadState.setStateLockless(i, 'E');
                        } finally {
                            threadState.setStateLockless(i, '*');
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
                            double runtime = (start - now) / 1000.0D;
                            long intervalTime = (now - lastTime);
                            long thisDirCount = countDir.get();
                            long thisFileCount = countFile.get();
                            long deltaDir = thisDirCount - lastDir;
                            long deltaFile = thisFileCount - lastFile;
                            double rateFile = deltaFile * 1_000_000_000.0 / intervalTime;
                            double avgFileRate = thisFileCount * 1_000_000_000.0 / (now - start);
                            long mb = countBytes.get() / (1024L * 1024L);
                            System.out.printf("%7s q-size: %,10d dir: %,10d (%,8d) file: %,10d (%,8d) file rate: %,6.0f  Avg rate: %,6.0f, MB: %,5d out-q: %,10d state: %s\n",
                                    humanTime(now - start, 2), queue.size(),
                                    thisDirCount, deltaDir,
                                    thisFileCount, deltaFile, rateFile, avgFileRate, mb, writeQueue.size(), threadState.toString());
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
                System.err.println("workers done - stopping writer at: " + writeQueue.size());
                writeQueue.put(Optional.empty());
                writer.join();
            }

        } catch (Exception e) {
            System.err.println("exception during runtime:");
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
