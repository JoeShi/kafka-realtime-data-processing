# 食亨大数据项目 PoC

## 场景描述及架构图

### 场景A

* 数据量大；流量稳定
* 业务逻辑简单
* 根据落盘时间作为分区字段，不需要合并

![](architect-shiheng-a.png)

### 场景B

先爬取最近90天的数据，通常是先不合并，然后计算一下，发现太慢，再人工触发合并，或者第二天进行合并。

* 数据量小
* 流量不可预测，可能是手动触发
* 根据订单时间落盘
* 如果发现查询慢，小文件合并成大文件；或者第二天触发合并
* 是否需要合并是配置选项

![](architect-shiheng-b.png)


## TODO

- [x] 构建 Kafka 和 Zookeeper 集群
- [x] 构建 EMR 集群， HIVE metadata store 放置于 RDS Aurora
- [x] Kafka 数据利用 Kafka Kinesis Connector 原样注入 Kinesis
- [x] 利用 Kafka Datagen Connector 产生模拟数据打入 Kafka 集群 
- [x] KCL 消费 Kinesis 的数据，铺平并根据订单时间落盘到 S3
- [x] Sparking Streaming 消费 Kinesis 数据，铺平并根据订单时间落盘到 S3
- [ ] 优雅地退出一个 Spark Streaming 的任务
- [x] 通过 s3-dist-cp 将小文件合并成一个大文件
- [x] Kinesis 原始数据通过 Firehose 落盘到 S3，保留原始数据
- [x] Hive 创建表，分区
- [ ] KCL 消费的时候，是否可以 rebalance
- [ ] Flume 会根据 topic 来进行分，不变得放一台, KCL 

### 如何使用 Kafka-connect-datagen 产生模拟数据

**AMI: ami-04eabc4c894294eb7**, 启动之后请增加 SG: sg-0215d847878dc26af, 和 IAM Role: S3-Connect-20191105014410665800000001

[Kafka-connect-datagen](https://github.com/confluentinc/kafka-connect-datagen) 是 Kafka 的一个 connector, 
可以用来产生模拟数据。

`worker.properties` 和 `orders.properties` 已经配置好。Order 的格式请参考[orders_schema.avro](orders_schema.avro), 
里面有一个嵌套 JSON, 需要在后续做铺平操作. 默认情况下会打入 **orders** topic.

执行以下脚本即可
```shell script
./confluent-5.3.1/bin/connect-standalone worker.properties orders.properties
```

### 如何使用 Kinesis-kafka-connector 将数据注入 Kinesis

**AMI: ami-056bad4b853478d85**， 启动之后请增加 SG: sg-0215d847878dc26af, 和 IAM Role: S3-Connect-20191105014410665800000001

[Kinesis-kafka-connector](https://github.com/awslabs/kinesis-kafka-connector) 可以用来将 Kafka 的数据打入到 Kinesis 中。

`worker.properties` 和 `kinesis.properties` 已经配置好。默认情况下会读取 **orders** topic，并打入叫 `shiheng-orders`。

执行以下脚本启动connect, **请务必使用 root 账号，`sudo su`**
```shell script
./confluent-5.3.1/bin/connect-standalone worker.properties kinesis.properties
```

## Kinesis 手动查看数据
```shell script
aws kinesis list-shards --stream-name shiheng-orders --profile shiheng
SHARD_ITERATOR=$(aws kinesis get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type LATEST --stream-name shiheng-orders --query 'ShardIterator' --profile shiheng)
aws kinesis get-records --profile shiheng --limit 10 --shard-iterator $SHARD_ITERATOR
```

## 编译 Kinesis-kafka-connector

操作系统：Amazon Linux 2

把已经编译好的 Jar 文件放入到 Confluent 中
```shell script
cp kinesis-kafka-connector/target/amazon-kinesis-kafka-connector-0.0.9-SNAPSHOT.jar ./confluent-5.3.1/share/java/kafka/
```

## EMR 操作指南

### 通过 Shell 提交 spark streaming 任务

和自建的 Spark 一样，可以通过 spark-submit 来提交任务

```shell script
spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.2 s3://shiheng-poc/scripts/spark-streaming-kinesis-python.py
```

### 通过 EMR Step 提交 spark streaming 任务
在生产环境中，推荐使用 EMR Step 来提交任务，如下截图:

![](assets/emr-step.png)


### 利用 s3-dist-cp 进行小文件合并

[S3DistCp (s3-dist-cp)](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html) 是一个开源工具，
争对 Amazon EMR 和 Amazon S3 进行了优化。可以实现文件的移动，或者将小文件合并成大文件。支持的场景丰富，常见用法可以
查看[Seven Tips for Using S3DistCp](https://aws.amazon.com/blogs/big-data/seven-tips-for-using-s3distcp-on-amazon-emr-to-move-data-efficiently-between-hdfs-and-amazon-s3/).

**合并文件，不删除**
```shell script
s3-dist-cp --src=s3://shiheng-poc/ss-kinesis-python/ordertime=2019111315/ \
--dest=s3://shiheng-poc/merged/ordertime=2019111315/ \
--groupBy='.*/(part)-.*\.csv' \
--targetSize=100 \
--outputManifest=processed-records.gz
```

**合并文件，成功后删除文件**
```shell script
s3-dist-cp --src=s3://shiheng-poc/ss-kinesis-python/ordertime=2019111316/ \
--dest=s3://shiheng-poc/merged/ordertime=2019111316/ \
--groupBy='.*/(part)-.*\.csv' \
--targetSize=100 \
--deleteOnSuccess \
--outputManifest=processed-records.gz
```

## Tips:

* You MUT create the database for hue `huedb` in external mysql before using [emrConfiguration.json](./emrConfiguration.json)


## KCL 代码消费落盘代码

[brianwwo/kclsample](https://github.com/brianwwo/kclsample)


## 参考资料

[Example AVRO schema file](https://github.com/confluentinc/kafka-connect-datagen/tree/master/src/main/resources)

[How to do graceful shutdown of spark streaming job](https://medium.com/@manojkumardhakad/how-to-do-graceful-shutdown-of-spark-streaming-job-9c910770349c)

[How to migrate a Hue database from an existing Amazon EMR cluster](https://aws.amazon.com/blogs/big-data/how-to-migrate-a-hue-database-from-an-existing-amazon-emr-cluster/)