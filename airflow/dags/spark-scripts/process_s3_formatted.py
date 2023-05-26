import sys
from pyspark.sql import SparkSession
import logging


def read_s3_file(s3_master_url, s3_url, file_path, aws_access_key_id, aws_secret_access_key):
    spark = SparkSession.builder\
        .appName("readS3")\
        .master(s3_master_url)\
        .getOrCreate()

    # Configure AWS credentials
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)

    # The S3 file path is the S3 URL concatenated with the file path
    s3_file_path = f'{s3_url}/{file_path}'
    df = spark.read.csv(s3_file_path)
    logging.info(df.show())
    # Add your DataFrame transformations here
    df.withColumnRenamed("city", "CITY").write.csv(s3_file_path)

    spark.stop()


if __name__ == "__main__":
    # The S3 URL and file path are passed as command-line arguments
    s3_master_url = sys.argv[1]
    s3_file_url = sys.argv[2]
    file_path = sys.argv[3]
    aws_access_key_id = sys.argv[4]
    aws_secret_access_key = sys.argv[5]
    read_s3_file(s3_master_url, s3_file_url, file_path, aws_access_key_id, aws_secret_access_key)
