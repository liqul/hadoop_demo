#!/usr/bin bash

#!/usr/bin/env bash

WORDDIR=`pwd`
NUM_OF_REDUCES=16
NUM_OF_SEQFILES=30
#each sort element cost 100 bytes
SIZE=$1 #total size of data in byte
FSIZE=$2 #size of file in byte

#output dir
SURFIX=$SIZE_$FSIZE
OUTPUT=raw_$SURFIX
CONTAINER="mycontainer"
SWIFT_SERVICE="SwiftTest"

#hdfs data upload dir
HDFSDIR=/tmp/llq

#swift data upload dir
SWIFTDIR="swift://$CONTAINER.$SWIFT_SERVICE/"

#jar file location
JAR=hadoop_demo-1.0-SNAPSHOT-jar-with-dependencies.jar

#zookeeper url
ZOO=192.168.130.32:2181

set -x #enable command echo
export HADOOP_USER_NAME=hdfs #export user name

echo "Step 4: run the CombineTeraInputFormat Terasort on data"
hdfs dfs -rm -r $HDFSDIR/combineOutput/
hdfs dfs -mkdir $HDFSDIR/combineOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/combineOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs $HDFSDIR/$OUTPUT $HDFSDIR/combineOutput/

echo "Step 5: run the CombineTeraInputFormat Terasort on HAR"
hdfs dfs -rm -r $HDFSDIR/harOutput/
hdfs dfs -mkdir $HDFSDIR/harOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/harOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs har://$HDFSDIR/data_$SURFIX.har/ $HDFSDIR/harOutput/

echo "Step 6: run TeraSort on sequence file"
hdfs dfs -rm -r $HDFSDIR/seqOutput/
hdfs dfs -mkdir $HDFSDIR/seqOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/seqOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES sq $HDFSDIR/seq_$SURFIX/ $HDFSDIR/seqOutput/

echo "Step 7: run TeraSort on HBase (HBase table need to be re-created before running the test)"
hdfs dfs -rm -r $HDFSDIR/hbaseOutput/
hdfs dfs -mkdir $HDFSDIR/hbaseOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/hbaseOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES -D hbase.zookeeper.quorum=$ZOO hb TeraSort_$SURFIX $HDFSDIR/hbaseOutput/

echo "Step 8: run TeraSort on Openstack Swift (TODO)"
#hdfs dfs -rm -r $HDFSDIR/swiftOutput/
#hdfs dfs -mkdir $HDFSDIR/swiftOutput/
#hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/swiftOutput/
#hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs $SWIFTDIR/$OUTPUT $HDFSDIR/swiftOutput/

echo "Done!"
