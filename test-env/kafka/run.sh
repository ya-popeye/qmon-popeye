#!/bin/sh
if [ "x$KAFKA_HOME" = "x" ]; then
  echo "need KAFKA_HOME"
fi
export SCALA_VERSION=2.10
me=$(pwd)/$(dirname $0)
num=$1
echo $num;
mkdir -p run/server$num;
rm -f run/server$num/*.log;
if [ "$num" = "zk" ]; then
(cd run/server$num && $KAFKA_HOME/bin/zookeeper-server-start.sh $me/config/zookeeper.properties)
else
(cd run/server$num && JMX_PORT=988$num $KAFKA_HOME/bin/kafka-server-start.sh $me/config/server${num}.properties)
fi
