#!/bin/sh

DIST_HOME="$(cd "$(cd "$(dirname "$0")"; pwd -P)"/..; pwd)"
JAVA_OPTS="$QMON_OPTS -Xms1024M -Xmx4096M -Xss1M -XX:MaxPermSize=256M -XX:+UseParallelGC"
clz=$1
shift

/usr/bin/env java $JAVA_OPTS \
  -cp "$DIST_HOME/config:@DIST_CLASSPATH@" \
  -Dqmon.logdir=${QMON_LOGDIR:-$DIST_HOME/logs} \
  -Dqmon.home="$DIST_HOME" \
  $clz "$@"
