#!/usr/bin/env bash

WORKDIR=`pwd`
SIZE=$1
FSIZE=$2
OUTPUT=$3

if [ ! -d $OUTPUT ]; then
    mkdir $OUTPUT
else
    rm -rf $OUTPUT
    mkdir $OUTPUT
fi

for b in $(seq 0 $FSIZE $(($SIZE-$FSIZE)))
do
    echo "generating data with offset $b"
    $WORKDIR/gensort -b$b $FSIZE $OUTPUT/part$b
done
