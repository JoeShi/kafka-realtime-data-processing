# PoC 资源共享

如果发现暂时可以避开，但后续需要解决的问题，务必记录在 [Issues](https://github.com/JoeShi/shiheng/issues) 里面.

项目 PoC 期间，使用 [Kanban](https://github.com/JoeShi/shiheng/projects/1) 来追踪项目进度。

## Kafka 集群 和 Zookeeper 集群
```
Kafka_List = 172.31.7.21:9092,172.31.31.118:9092,172.31.40.206:9092
Zookeeper_List = 172.31.8.225:2181,172.31.18.253:2181,172.31.47.133:2181
```

## Kafka 操作

### 手动操作

**创建 topic**
```shell script
./confluent-5.3.1/bin/kafka-topics --zookeeper 172.31.8.225:2181,172.31.18.253:2181,172.31.47.133:2181 --create --partitions 3 --replication-factor 2 --topic topicName 
```

**消费 topic**
```shell script
./confluent-5.3.1/bin/kafka-console-consumer --bootstrap-server 172.31.7.21:9092,172.31.31.118:9092,172.31.40.206:9092 --topic orders
```

**手动打数据**
```shell script
./confluent-5.3.1/bin/kafka-console-producer --broker-list 172.31.7.21:9092,172.31.31.118:9092,172.31.40.206:9092 --topic topicName
```

## Hue

* username：admin
* password: admin@AWS123


