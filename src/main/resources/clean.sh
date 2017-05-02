#!/usr/bin bash

OUTPUT=$1
HDFSDIR=$2
SWIFTDIR=$3

echo "Step 9: delete output files"
rm -rf $OUTPUT
hdfs dfs -rm -r $HDFSDIR
hdfs dfs -rm -r $SWIFTDIR/$OUTPUT