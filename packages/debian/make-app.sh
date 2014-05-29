#!/bin/sh

me=$(dirname $0)
for f in $(ls $me/template/*); do
  fp=$(basename $f)
  nf=$(echo $fp | sed "s,^qmon-popeye,qmon-popeye-$1,")
  cat $f | sed "s/@APP_NAME@/$1/g" > $me/$nf
done
