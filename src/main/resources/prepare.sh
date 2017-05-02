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

echo "step 0: clean existing files"
rm -rf $OUTPUT
hdfs dfs -rm -r $HDFSDIR
hdfs dfs -rm -r $SWIFTDIR/$OUTPUT

echo "Step 1: generate sort data"
sh pgensort.sh $SIZE $FSIZE $OUTPUT

echo "Step 2: upload to hdfs"
hdfs dfs -mkdir $HDFSDIR
hdfs dfs -put $OUTPUT $HDFSDIR

echo "Step 3: run the combine TeraSort on data to obtain the _partition.list"
hdfs dfs -mkdir $HDFSDIR/rawOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs $HDFSDIR/$OUTPUT $HDFSDIR/rawOutput/

echo "Step 4: archiving to HAR"
hadoop archive -archiveName data_$SURFIX.har -p $HDFSDIR/$OUTPUT $HDFSDIR/

echo "Step 5: archiving to sequence file"
hadoop jar $JAR tool.SequenceFileMerger -D mapreduce.job.reduces=$NUM_OF_SEQFILES $HDFSDIR/$OUTPUT $HDFSDIR/seq_$SURFIX/

echo "Step 7: create and put data into TeraSort_$SURFIX table on HBase"
echo "disable 'TeraSort_$SURFIX'" | hbase shell
echo "drop 'TeraSort_$SURFIX'" | hbase shell
echo "create 'TeraSort_$SURFIX', {NAME=>'data',IS_MOB=>true}" | hbase shell
hadoop jar $JAR tool.HbaseTeraImporter -D hbase.zookeeper.quorum=$ZOO $HDFSDIR/$OUTPUT TeraSort_$SURFIX

echo "Step 8: upload to Openstack Swift"
hdfs dfs -put $OUTPUT $SWIFTDIR

echo "Done!"
