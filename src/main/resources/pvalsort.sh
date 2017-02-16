#!/usr/bin/env bash

WORKDIR=`pwd`
NUM=$1
OUTPUT=$2
TEMPLATE="part-r-0000"
if [ $1 -eq "1" ]; then
    $WORKDIR/valsort $OUTPUT/$TEMPLATE0
    exit
fi

all=""
for p in $(seq 0 1 $(($1-1)))
do
    echo "validating part $TEMPLATE$p"
    $WORKDIR/valsort -o out$p $OUTPUT/$TEMPLATE$p
    all="$all out$p"
done
cat $all > all.sum
$WORKDIR/valsort -s all.sum
rm $all
rm all.sum-bash: /var/log/audit.log: Permission denied
