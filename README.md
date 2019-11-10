# 食亨大数据项目 PoC

p.s. 在没有任何特殊说明下

## 开发资源共享

操作系统：Amazon Linux 2

以下是 Kafka 集群和 Zookeeper 集群的共享信息
```
Kafka_List = 172.31.7.21:9092,172.31.31.118:9092,172.31.40.206:9092
Zookeeper_List = 172.31.8.225:2181,172.31.18.253:2181,172.31.47.133:2181
```

**创建 topic **
```shell script
./confluent-5.3.1/bin/kafka-topics --zookeeper 172.31.8.225:2181,172.31.18.253:2181,172.31.47.133:2181 --create --partitions 3 --replication-factor 2 --topic topicName 
```

**消费 topic **
```shell script
./confluent-5.3.1/bin/kafka-console-consumer --bootstrap-server 172.31.7.21:9092,172.31.31.118:9092,172.31.40.206:9092 --topic topicName
```

## 如何使用 Kafka-connect-datagen 产生模拟数据

**AMI: ami-04eabc4c894294eb7**

[Kafka-connect-datagen](https://github.com/confluentinc/kafka-connect-datagen) 是 Kafka 的一个 connector, 可以用来产生模拟数据。

`worker.properties` 和 `orders.properties` 已经配置好。Order 的格式请参考[orders_schema.avro](orders_schema.avro), 里面有一个
嵌套 JSON, 需要在后续做铺平操作. 默认情况下会打入 **orders** topic.


执行以下脚本即可
```shell script
./confluent-5.3.1/bin/connect-standalone worker.properties orders.properties
```

## 如何使用 Kinesis-kafka-connector 将数据注入 Kinesis

**AMI: ami-056bad4b853478d85**

[Kinesis-kafka-connector](https://github.com/awslabs/kinesis-kafka-connector) 可以用来将 Kafka 的数据打入到 Kinesis 中。

`worker.properties` 和 `kinesis.properties` 已经配置好。默认情况下会读取 **orders** topic，并打入叫 `shiheng-orders`。

执行以下脚本启动connect
```shell script
./confluent-5.3.1/bin/connect-standalone worker.properties kinesis.properties
```


## 编译 Kinesis-kafka-connector

把已经编译好的 Jar 文件放入到 Confluent 中
```shell script
cp kinesis-kafka-connector/target/amazon-kinesis-kafka-connector-0.0.9-SNAPSHOT.jar ./confluent-5.3.1/share/java/kafka/
```



## 参考资料

https://github.com/confluentinc/avro-random-generator/blob/master/src/main/java/io/confluent/avro/random/generator/Generator.java



[Example AVRO schema file](https://github.com/confluentinc/kafka-connect-datagen/tree/master/src/main/resources)

