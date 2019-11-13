import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import time
from pyspark.sql.types import Row

ssc = StreamingContext(sc, 5)
streamName = 'shiheng-orders'
appName = 'shiheng-orders'
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
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda w: Row(ordertime=w[0],orderid=w[1],itemid=w[2],orderunits=w[3],address_city=w[4],address_state=w[5],address_zipcode=w[6]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("df")
        # df.write.partitionBy("ordertime").parquet("s3n://shiheng-poc/maweijun/data1")
        df.write.partitionBy("ordertime").csv("s3n://shiheng-poc/maweijun/data2")
        results = spark.sql("SELECT * FROM df")
        results.show()
    except:
        pass

dstream = KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.TRIM_HORIZON, 2)

py_rdd_stream = dstream.map(lambda x:(convertTime(json.loads(x)["ordertime"]),json.loads(x)["orderid"],json.loads(x)["itemid"],json.loads(x)["orderunits"],json.loads(x)["address"]["city"],json.loads(x)["address"]["state"],json.loads(x)["address"]["zipcode"]))

# py_rdd_stream = dstream.map(lambda x:(json.loads(x)["ordertime"],(json.loads(x)["orderid"],json.loads(x)["itemid"],json.loads(x)["orderunits"],json.loads(x)["address"]["city"],json.loads(x)["address"]["state"],json.loads(x)["address"]["zipcode"])))
py_rdd_stream.pprint(10)

# py_rdd_stream.foreachRDD(lambda rdd: rdd.foreachPartition(save2s3))
py_rdd_stream.foreachRDD(save2s3)

# py_rdd_stream.saveAsTextFiles("s3n://shiheng-poc/maweijun/data")

ssc.start()
ssc.awaitTermination()
