package org;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import picocli.CommandLine;

import java.io.*;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

public class Cmds {
    public static class Cli {
//        @CommandLine.Parameters(index="0", description = "top or root path to descend into")
//        String rootDir;

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

    public static boolean debug = false;
    public static FileSystem fs;
    public static StringBuilder bld = new StringBuilder(64);
    public static DateFormat df  = DateFormat.getDateInstance();

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

            fs = Util.getHdfsFileSystem(debug, cli.krbUser, cli.coreSitePath, cli.hdfsSitePath, cli.krb5Path, cli.krbKeyTab);

                BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));
                System.out.print("prompt $:");
                String cmd = rdr.readLine();
                String lastTrace = "";
                while(cmd != null) {
                    // System.out.println("cmd >>: " + cmd);
                    String[] params = cmd.split("\\s+");
                    if ( cmd.matches("\\s*\\?\\s*")) {
                        System.out.println(lastTrace);
                    } else if ( cmd.length() <= 0 || params.length <= 0 )
                        System.out.printf("cmd not usable: \"%s\"\n", cmd);
                    else {
                        try {
                            switch (params[0]) {
                                case "ls":
                                    ls(params);
                                    break;
                                case "get":
                                    get(params);
                                    break;
                                case "put":
                                    put(params);
                                    break;
                            }

                        } catch (Exception e) {
                            try(ByteArrayOutputStream bos = new ByteArrayOutputStream(16*1024)) {
                                PrintWriter pr = new PrintWriter(bos);
                                e.printStackTrace(pr);
                                pr.flush();
                                bos.flush();
                                lastTrace = "Trace from cmd: " + cmd + "\nLast Stack Trace: \n" + bos.toString("UTF-8");
                            }
                            System.out.printf("error on cmd: \"%s\" >> %s\n", cmd, e.getMessage());
                        }
                    }
                    System.out.print("prompt $:");
                    cmd = rdr.readLine();

                }



        }catch (Throwable e) {
            System.err.println("runtime exception:");
            e.printStackTrace();
        }
    }

    private static void put(String[] params) throws IOException {
        if ( params.length != 3)
            throw new RuntimeException("put requires 2 parameters - localSrcFilePath hdfsDstFilePath");
        String local = params[1];
        String dest = params[2];
        File file = new File(local);
        if ( !file.exists())
            throw new RuntimeException("local file: " + local + " does not exist");
        if ( !file.isFile())
            throw new RuntimeException("local file: " + local + " is not a regular file");
        if ( fs.exists(new Path(dest)))
            throw new RuntimeException("destination file: " + dest + " already exists");
        fs.copyFromLocalFile(false, new Path(local), new Path(dest));
    }


    private static void get(String[] params) throws IOException {
        if ( params.length != 3)
            throw new RuntimeException("put requires 2 parameters - localSrcFilePath hdfsDstFilePath");
        String src = params[1];
        String local = params[2];
        File file = new File(local);
        if ( file.exists())
            throw new RuntimeException("local file: " + local + " already exists");
        if ( !fs.exists(new Path(src)))
            throw new RuntimeException("hdfs file: " + src + " does not exist");
        if ( !fs.isFile(new Path(src)))
            throw new RuntimeException("hjdfs file: " + src + " is not aq regular file");
        fs.copyToLocalFile(false, new Path(src), new Path(local));
    }

    private static void ls(String[] params) throws IOException {
        if ( params.length < 1)
            throw new RuntimeException("No params passed to ls command");
        for (int i = 1; i < params.length; i++) {
            FileStatus[] list = fs.listStatus(new Path(params[i]));
            for(FileStatus stat: list) {
                System.out.println(lsAFile(stat));
            }
        }
    }

    public static String lsAFile(FileStatus stat) {


        bld.setLength(0);
        if (stat.isDirectory()) {
            bld.append("D\t");
//                                    stat.dircount++;
        } else if (stat.isSymlink()) {
            bld.append("S\t");
//                                    stat.size += entry.getLen();
//                                    stat.filecount++;
//                                    stat.oldest = Math.max(entry.getModificationTime(), stat.oldest);
//                                    stat.youngest = Math.min(entry.getModificationTime(), stat.youngest);
        } else {
            bld.append("F\t");
        }
        bld.append(stat.getPath())
                .append('\t')
                .append(stat.getLen())
                .append('\t')
                .append(df.format(new Date(stat.getModificationTime())))
                .append('\t')
                .append(stat.getOwner());
        // trying to offload UTF 8 encoding in worker thread
        return bld.toString();
    }

}
