Documentation is obsolete, update is coming soon.

qmon-popeye
===========

Event transport for QMon prototype

Building
===========

As usual, sbt dist

Running
=======

To able to run anything, following steps are needed:

1. run kafka from https://github.com/octo47/kafka/tree/0.8-scala2.10

2. create topic, number of partitons defines number of threads from one group, able to read topic in parallel. Expected to be > 10 x (consumers in 1 group)

```
bin/kafka-create-topic.sh --zookeeper <zk.host1:2181,zk.host2:2181> --topic popeye-points --partition <partitons>
```
3. Create configuration file config/application.conf (also possible to create pump.conf/slicer.conf in case of different settings per daemon)

```
zk.cluster = "<zk.host1:port,zk.host2:port>"
kafka.metadata.broker.list = "kafka.host1:9092,kafka.host2:9092"

generator {
   worker = <unique id generator>
   datacenter = <datacenter id>
}
```

