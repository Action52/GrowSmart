import pyspark.sql.functions as F

from probables import (CountMinSketch)
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit, avg
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType

class Spark():
    '''
        Spark session client.
    '''
    def _init_session(self):
        spark = SparkSession \
            .builder \
            .appName("Kafka batch query") \
            .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("ERROR")
        return spark

class SparkStreaming(Spark):
    '''
        Spark streaming client.
    '''
    KAFKA_TOPIC_NAME_CONS = "iot_devices_data"
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:29092'

    def init_streaming(self):
        self.sc = Spark._init_session(self)
        df = self.sc.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP_SERVERS_CONS) \
            .option("subscribe", self.KAFKA_TOPIC_NAME_CONS) \
            .option("startingOffsets", "earliest") \
            .load()
        return df
    
    def get_sc(self):
        return self.sc

class StreamAnalytics():
    '''
        Class for doing stream analytics from the stream.
    '''
    
    # Variable for Sliding Windows
    WINDOW_SIZE = "5 minutes"
    SLIDE_DURATION = "5 minutes"
    
    # Width and Depth for Count min sketch algorithm
    WIDTH = 1000
    DEPTH = 5
    
    # Top K for Heavy Hitters
    K = 3
    
    # Elastic seach host
    ES_HOST = 'http://localhost:9200'
    ES_USER = 'elastic'
    ES_PASS = 'changeme'
    
    # Costant for ES index
    COUNT_IDX = 'most-active-gardens'
    SENSOR_IDX = 'sensor-analytics'
    
    # Schema for the data
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
        StructField("soil_phosporous", DoubleType(), False),
        StructField("garden_id", StringType(), False),
        StructField("garden_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("species_id", StringType(), False),
        StructField("plant_id", StringType(), False),
        StructField("timestamp", StringType(), False),
    ])

    def __init__(self):
        self.spark_stream = SparkStreaming()
        self.stream = self.spark_stream.init_streaming()
        self.cms = CountMinSketch(self.WIDTH, self.DEPTH)
        self.heavy_hitters = {}
        self.es = Elasticsearch([self.ES_HOST], http_auth=(self.ES_USER, self.ES_PASS))
        
    def _update_cms(self, rows, batch_id):
        '''
            This helper method will implement Heavy hitters with the help count min sketch for counting the garden id.
        '''
        row_collect = rows.collect()
        
        # Update count min sketch and heavy hitters
        for row in row_collect:
            garden_id = str(row["garden_id"])
            
            # Add to count  min sketch
            if garden_id in self.heavy_hitters:
                self.heavy_hitters[garden_id] += 1
                self.cms.add(garden_id, 1)
                continue
            else:
                self.cms.add(garden_id, int(row['count']))
                
            count = self.cms.check(garden_id)

            # Check if the count exceeds the threshold
            if len(self.heavy_hitters) < self.K:
                self.heavy_hitters[garden_id] = count
            # Update the heavy hitters
            else:
                curr_min = min(self.heavy_hitters, key=self.heavy_hitters.get)
                min_freq = self.heavy_hitters[curr_min]
                
                if count > min_freq:
                    del self.heavy_hitters[curr_min]
                    self.heavy_hitters[garden_id] = count
            

        # Send heavy hitters to Elasticsearch
        for garden_id, count in self.heavy_hitters.items():
            document = {
                'garden_id': garden_id,
                'count': count
            }
            self.es.index(index=self.COUNT_IDX, body=document) 

        # Refresh the Elasticsearch index
        self.es.indices.refresh(index=self.COUNT_IDX)


    def count_stream(self):
        '''
        This method is to count the stream by the garden name,
        it will retrieve the stream data directly from the Kafka stream
        and apply count query on the fly.
        
        The count will be save in the Countminsketch data structure and we will be using heavy hitters algorithm
        to determine top 3 most popular garden.
        '''
        query = self.stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), self.SCHEMA).alias("value")) \
            .select("value.*", to_timestamp(lit(F.col("value.timestamp")), "yyyy-MM-dd_HH-mm-ss").alias("parsed_timestamp")) \
            .groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "garden_id") \
            .count() \
            .withColumnRenamed("window", "time_window") \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .foreachBatch(self._update_cms) \
            .start()

        query.awaitTermination()
    
    def _write_to_elasticsearch(self, df, batch_id):
        data = df.toJSON().collect()
        for record in data:
            self.es.index(index=self.SENSOR_IDX,  body=eval(record))
            
        self.es.indices.refresh(index=self.SENSOR_IDX)

    def sensor_analytics(self):
        '''
            This method will aggregate average data collected from sensor from the 5 minutes windows.
        '''
        non_grouped_query = self.stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), self.SCHEMA).alias("value")) \
            .select("value.*", to_timestamp(lit(F.col("value.timestamp")), "yyyy-MM-dd_HH-mm-ss").alias("parsed_timestamp")) \
            .withWatermark("parsed_timestamp", "5 minutes")
        window_query = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION))

        # Querying the aggregation
        temp = window_query.agg(avg("temp").alias("average_temperature"))
        co = window_query.agg(avg("co").alias("average_co"))
        humidity = window_query.agg(avg("humidity").alias("average_humidity"))
        smoke = window_query.agg(avg("smoke").alias("average_smoke"))
        lpg = window_query.agg(avg("lpg").alias("average_lpg"))
        rainfall = window_query.agg(avg("rainfall").alias("average_rainfall"))
        soil_temp = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_temp").alias("average_soil_temp"))
        soil_humidity = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_humidity").alias("average_soil_humidity"))
        soil_nitrogen = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_nitrogen").alias("average_soil_nitrogen"))
        soil_potassium = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_potassium").alias("average_soil_potassium"))
        soil_phosporous = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_phosporous").alias("average_soil_phosporous"))
        soil_ph = non_grouped_query.groupBy(F.window("parsed_timestamp", self.WINDOW_SIZE, self.SLIDE_DURATION), "species_id").agg(avg("soil_ph").alias("average_soil_ph"))

        # Write to the elastic search
        temp_query = temp.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        co_query = co.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        humidity_query = humidity.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        smoke_query = smoke.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        lpg_query = lpg.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        rainfall_query = rainfall.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_temp_query = soil_temp.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_humidity_query = soil_humidity.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_nitrogen_query = soil_nitrogen.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_potassium_query = soil_potassium.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_phosporous_query = soil_phosporous.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        soil_ph_query = soil_ph.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_elasticsearch) \
            .start()

        temp_query.awaitTermination()
        co_query.awaitTermination()
        humidity_query.awaitTermination()
        smoke_query.awaitTermination()
        lpg_query.awaitTermination()
        rainfall_query.awaitTermination()
        soil_temp_query.awaitTermination()
        soil_humidity_query.awaitTermination()
        soil_nitrogen_query.awaitTermination()
        soil_potassium_query.awaitTermination()
        soil_phosporous_query.awaitTermination()
        soil_ph_query.awaitTermination()

if __name__ == "__main__":
    stream_analytics = StreamAnalytics()
    stream_analytics.sensor_analytics()
    stream_analytics.count_stream()
