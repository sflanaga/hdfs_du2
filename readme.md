#Multithread HDFS file lister

This application does a listing of all files recursively under one directory, and is intended to be used with massive file systems.

It does this fast by dividing each directory it files as a job and re-queues those directories to be looked at by other threads.

The BossWorkerQueue is a kind of co-op queue where each thread does work, but it also finds new work to do.

It is also an example of authentication off-edge OR not on an edge node.

The original routine for that FileSystem creation is taken from Karthik.Vasudevan2@T-Mobile.com's code.

Normally, on an edge node, you would just set the classpath by using hdfs classpath, but when not on an edge node this routine prescribes the minimal set of configuration to access hdfs.

To run this example, you must build the app jar separate from the 3rd party jars.

`mvn clean install`
`mvn mvn dependency:copy-dependencies` # create a directory in which all the 3rd party jars required to run are placed.

copy the main jar to a directory and the 3rd party ones to a lib directory

`java -cp hdfs_du2-1.0-SNAPSHOT.jar:lib/* org.HdfsDu2 /prod test --krb5_user adm_sflanag1@HDPQUANTUMPROD.COM --krb5_key_tab /etc/security/keytabs/adm_sflanag1.user.keytab`

This will write the list of files to test file.