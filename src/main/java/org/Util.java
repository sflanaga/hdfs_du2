package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;
import java.util.stream.Collectors;

public class Util {
    public static FileSystem getHdfsFileSystem(boolean debug, String krbUser, String coreSiteXml, String hdfsSiteXml, String krb5confLoc, String keyTabLoc) {
        try {

            Configuration config = new Configuration();

            config.addResource(new org.apache.hadoop.fs.Path(coreSiteXml));
            config.addResource(new org.apache.hadoop.fs.Path(hdfsSiteXml));
            config.set("hadoop.security.authentication", "Kerberos");
            config.addResource(krb5confLoc);
            config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            System.setProperty("java.security.krb5.conf", krb5confLoc);
            org.apache.hadoop.security.HadoopKerberosName.setConfiguration(config);
            UserGroupInformation.setConfiguration(config);
            UserGroupInformation.loginUserFromKeytab(krbUser, keyTabLoc);

            if ( debug ) {
                List<String> list = config.getValByRegex(".*").keySet().stream().sorted().collect(Collectors.toList());
                for (String e : list) {
                    System.out.printf("HDFS conf: %s = %s\n", e, config.get(e));
                }
            }


            return FileSystem.get(config);
        } catch (Exception ex) {
            throw new RuntimeException("error getting hdfs fs setup: " + ex.getMessage(), ex);
        }
    }
}
