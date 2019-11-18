import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import time
from pyspark.sql.types import Row
from pyspark.sql import SparkSession

sc = SparkContext()
ssc = StreamingContext(sc, 60)
streamName = 'orders'
appName = 'orders-python' # This will be the DynamoDB Table name.
endpointUrl = "https://kinesis.cn-northwest-1.amazonaws.com.cn"
regionName = "cn-northwest-1"

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def convertTime(timestamp):
    return time.strftime("%Y%m%d%H", time.localtime(timestamp))

def save2s3(rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    if rdd.isEmpty():
        return
    rowRdd = rdd.map(lambda w: Row(ordertime=w[0],orderid=w[1],itemid=w[2],orderunits=w[3],address_city=w[4],address_state=w[5],address_zipcode=w[6]))
    df = spark.createDataFrame(rowRdd)
    df.createOrReplaceTempView("df")
    df.write.partitionBy("ordertime").csv(path="s3n://joeshi-poc/ss-kinesis-python/", mode="append")
    # results = spark.sql("SELECT * FROM df")
    # results.show()

dstream = KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

py_rdd_stream = dstream.map(lambda x:(convertTime(json.loads(x)["ordertime"]),json.loads(x)["ordertime"],json.loads(x)["orderid"],json.loads(x)["itemid"],json.loads(x)["orderunits"],json.loads(x)["address"]["city"],json.loads(x)["address"]["state"],json.loads(x)["address"]["zipcode"]))

# py_rdd_stream.pprint(10)

py_rdd_stream.foreachRDD(save2s3)

ssc.start()
ssc.awaitTermination()

# spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.2 s3://shiheng-poc/scripts/spark-streaming-kinesis-python.py