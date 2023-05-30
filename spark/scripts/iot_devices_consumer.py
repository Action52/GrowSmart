import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType

class Spark():
    def _init_session(self):
        spark = SparkSession \
            .builder \
            .appName("Kafka batch query") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("ERROR")
        return spark

class SparkStreaming(Spark):
    KAFKA_TOPIC_NAME_CONS = "iot_devices_data"
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:29092'

    def _init_streaming(self):
        stream = Spark._init_session(self)
        df = stream.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP_SERVERS_CONS) \
            .option("subscribe", self.KAFKA_TOPIC_NAME_CONS) \
            .option("startingOffsets", "earliest") \
            .load()
        return df

class StreamAnalytics():
    SCHEMA = StructType([
        StructField("device_id", StringType(), False),
        StructField("light", BooleanType(), False),
        StructField("motion", BooleanType(), False),
        StructField("co", DoubleType(), False),
        StructField("humidity", DoubleType(), False),
        StructField("smoke", DoubleType(), False),
        StructField("temp", DoubleType(), False),
        StructField("lpg", DoubleType(), False),
        StructField("soil_ph", DoubleType(), False),
        StructField("rainfall", DoubleType(), False),
        StructField("soil_temp", DoubleType(), False),
        StructField("soil_humidity", DoubleType(), False),
        StructField("soil_nitrogen", DoubleType(), False),
        StructField("soil_potassium", DoubleType(), False),
        StructField("soil_phosphorus", DoubleType(), False),
        StructField("garden_id", StringType(), False),
        StructField("garden_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("species_id", StringType(), False),
        StructField("plant_id", StringType(), False)
    ])

    def __init__(self):
        self.stream = SparkStreaming()._init_streaming()

    def count_stream(self):
        '''
            This method is to count the stream by the garden name,
            it will retrieve the stream data directly from the kafka stream
            and applying count query on the fly.
        '''
        self.stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), self.SCHEMA).alias("value")) \
            .select("value.*") \
            .select(F.col("garden_name")) \
            .groupBy("garden_name") \
            .count() \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start() \
            .awaitTermination()

if __name__ == "__main__":
    stream_analytics = StreamAnalytics()
    stream_analytics.count_stream()
