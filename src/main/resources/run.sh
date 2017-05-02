#!/usr/bin/env bash

WORDDIR=`pwd`
NUM_OF_REDUCES=8
NUM_OF_SEQFILES=10
#each sort element cost 100 bytes
SIZE=$1 #total size of data in byte
FSIZE=$2 #size of file in byte

#output dir
OUTPUT=raw
CONTAINER="mycontainer"
SWIFT_SERVICE="SwiftTest"

#hdfs data upload dir
HDFSDIR=/tmp/llq

#swift data upload dir
SWIFTDIR="swift://$CONTAINER.$SWIFT_SERVICE/"

#jar file location
JAR=hadoop_demo-1.0-SNAPSHOT-jar-with-dependencies.jar

#zookeeper url
ZOO=192.168.130.100:2181

set -x #enable command echo
export HADOOP_USER_NAME=hdfs #export user name

echo "Step 0: clean existing files"
sh clean.sh $OUTPUT $HDFSDIR $SWIFTDIR

echo "Step 1: generate sort data"
sh pgensort.sh $SIZE $FSIZE $OUTPUT

#check the generated data before continue
#read -p "Press [Enter] key to start upload data to hdfs..."

echo "Step 2: upload to hdfs"
hdfs dfs -mkdir $HDFSDIR
hdfs dfs -put $OUTPUT $HDFSDIR

echo "Step 3: run the raw TeraSort on data"
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES raw $HDFSDIR/raw/ $HDFSDIR/rawOutput/

echo "Step 4: run the CombineTeraInputFormat Terasort on data"
hdfs dfs -mkdir $HDFSDIR/combineOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/combineOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs $HDFSDIR/raw/ $HDFSDIR/combineOutput/

echo "Step 5: run the CombineTeraInputFormat Terasort on HAR"
hdfs dfs -mkdir $HDFSDIR/harOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/harOutput/
hadoop archive -archiveName data.har -p $HDFSDIR/raw/ $HDFSDIR/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs har://$HDFSDIR/data.har/ $HDFSDIR/harOutput/

echo "Step 6: run TeraSort on sequence file"
hdfs dfs -mkdir $HDFSDIR/seqOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/seqOutput/
hadoop jar $JAR tool.SequenceFileMerger -D mapreduce.job.reduces=$NUM_OF_SEQFILES $HDFSDIR/raw/ $HDFSDIR/seq/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES sq $HDFSDIR/seq/ $HDFSDIR/seqOutput/

echo "Step 7: run TeraSort on HBase (HBase table need to be re-created before running the test)"
echo "disable 'TeraSort'" | hbase shell
echo "drop 'TeraSort'" | hbase shell
echo "create 'TeraSort', {NAME=>'data',IS_MOB=>true}" | hbase shell
hdfs dfs -mkdir $HDFSDIR/hbaseOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/hbaseOutput/
hadoop jar $JAR tool.HbaseTeraImporter -D hbase.zookeeper.quorum=$ZOO $HDFSDIR/raw/ TeraSort
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES -D hbase.zookeeper.quorum=$ZOO hb TeraSort $HDFSDIR/hbaseOutput/

echo "Step 8: run TeraSort on Openstack Swift (TODO)"
hdfs dfs -put $OUTPUT $SWIFTDIR
hdfs dfs -mkdir $HDFSDIR/swiftOutput/
hdfs dfs -cp $HDFSDIR/rawOutput/_partition.lst $HDFSDIR/swiftOutput/
hadoop jar $JAR MyTeraSort -D mapreduce.job.reduces=$NUM_OF_REDUCES fs $SWIFTDIR/raw/ $HDFSDIR/swiftOutput/

echo "Done!"
