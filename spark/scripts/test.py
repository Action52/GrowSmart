from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
rdd = spark.sparkContext.parallelize(data)


dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
