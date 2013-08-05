qmon-popeye
===========

Event transport for QMon prototype

Building
===========

Kafka used as project dependency: https://github.com/octo47/kafka/tree/0.8-scala2.10 (fetched automatically by sbt)

Running
=======

To able to run anything, following steps are needed:

1. running kafka from https://github.com/octo47/kafka/tree/0.8-scala2.10

2. create topic

```
bin/kafka-create-topic.sh --zookeeper <zk.host1:port,zk.host2:port> --topic popeye-points --partition <partitons>
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

