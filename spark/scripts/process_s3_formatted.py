import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit, to_date, unix_timestamp, current_date, col, concat
from pyspark.sql import DataFrame
import logging
from datetime import date


def create_edges(df: DataFrame, origin, end, edgelabel):
    edges = df.select(origin, end) \
        .distinct() \
        .withColumnRenamed(origin, ":START_ID") \
        .withColumnRenamed(end, ":END_ID") \
        .withColumn(":TYPE", lit(edgelabel))
    return edges


def create_column_data_labels(df: DataFrame, original_col, cols_to_select):
    return df.select(cols_to_select)\
            .distinct()\
            .withColumn(":ID", concat(lit(original_col), monotonically_increasing_id()))\
            .withColumn(":LABEL", lit(original_col))


def transform(s3_master_url, s3_url, file_path, aws_access_key_id, aws_secret_access_key):
    spark = SparkSession.builder\
        .appName("readS3")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4')\
        .config('fs.s3a.endpoint', 's3.eu-west-3.amazonaws.com') \
    .getOrCreate()

    # The S3 file path is the S3 URL concatenated with the file path
    s3_file_path = f'{s3_url}/{file_path}'
    print(s3_file_path)
    df = (
        spark.read.csv(s3_file_path, header=True, inferSchema=False)
        .withColumn("prediction_date", to_date(unix_timestamp("prediction_date", "dd/MM/yy").cast("timestamp")))
        .withColumn("today", lit(current_date()))
    )

    # Generate the node dfs (:ID,  attr:type, ... ,  :LABEL)

    nodes_garden = df.select("garden_id", "garden_name", "city")\
        .distinct()\
        .withColumn(":ID", concat(lit("GARDEN"), col("garden_id")))\
        .withColumn(":LABEL", lit("Garden"))

    nodes_device = df.select("sensor_id")\
        .distinct()\
        .withColumn(":ID", concat(lit("SENSOR"), col("sensor_id")))\
        .withColumn(":LABEL", lit("Device"))

    nodes_plant = df.select("plant_id")\
        .distinct() \
        .withColumn(":ID", concat(lit("PLANT"), col("plant_id"))) \
        .withColumn(":LABEL", lit("Plant"))

    nodes_species = df.select("species_id60")\
        .distinct() \
        .withColumn(":ID", concat(lit("SPECIES"), col("species_id60"))) \
        .withColumn(":LABEL", lit("Species"))

    nodes_location = df.select("city")\
        .distinct() \
        .withColumn(":ID", concat(lit("LOCATION"), col("city")))\
        .withColumn(":LABEL", lit("Location"))

    weather_cols = ["prediction_date", 'temperature_2m_max', 'temperature_2m_min', 'rain_sum', 'showers_sum',
                    'snowfall_sum', 'precipitation_probability_max', 'city']
    nodes_weather = df.select(weather_cols)\
        .distinct()\
        .withColumn(":ID", concat(lit("WEATHER"), col("city"), col("prediction_date")))\
        .withColumn(":LABEL", lit("WeatherForecast"))

    nodes_today_date = df.select("today")\
        .distinct()\
        .withColumn(":ID", concat(lit("DATE"), col("today")))\
        .withColumn(":LABEL", lit("Date"))
    nodes_predicted_date = df.select("prediction_date")\
        .distinct()\
        .withColumn(":ID", concat(lit("DATE"), col("prediction_date")))\
        .withColumn(":LABEL", lit("Date"))

    nodes_date = nodes_today_date.union(nodes_predicted_date).distinct()

    sensor_data_cols = ["sensor_id", "today", "light", "motion", "co", "humidity", 'smoke', 'temp', 'lpg', "soil_ph",
                        "rainfall", "soil_temp", "soil_humidity", "soil_nitrogen", "soil_potassium",
                        "soil_phosporous"]

    nodes_event = df.select(sensor_data_cols)\
        .distinct()\
        .withColumn(":ID", concat(lit("EVENT"), monotonically_increasing_id()))\
        .withColumn(":LABEL", lit("Event"))\
        .select(":ID", "sensor_id", "today", ":LABEL")

    nodes_sensor_data = [create_column_data_labels(df, sensor, [sensor, "sensor_id"])
                         for sensor in sensor_data_cols if sensor not in ["sensor_id"]]

    plant_data_cols = ["Species_name_standardized_against_TPL", "Taxonomic_level",
                       "Status_according_to_TPL", "Genus", "Phylogenetic_Group_within_angiosperms",
                       "Phylogenetic_Group_General", "Adaptation_to_terrestrial_or_aquatic_habitats",
                       "Woodiness", "Growth_Form", "Succulence", "Nutrition_type_parasitism",
                       "Nutrition_type_carnivory", "Leaf_type", "Leaf_area_mm2", "Leaf_area_n_o",
                       "Plant_height_m", "Plant_height_n_o"]

    nodes_plant_data = [create_column_data_labels(df, plant, [plant, "species_id60"]) for plant in plant_data_cols]

    node_dfs = [nodes_garden, nodes_plant, nodes_device, nodes_species, nodes_location,
                nodes_weather, nodes_date, nodes_event]
    node_dfs.extend(nodes_sensor_data)
    node_dfs.extend(nodes_plant_data)

    for i, node_df in enumerate(node_dfs):
        # Generate the S3 file path for this node DataFrame
        s3_file_path = f"{s3_url}/node_df_{i}.csv"
        # Write the node DataFrame to a CSV file in S3
        node_df.write.csv(s3_file_path, mode="overwrite")

    # Now create the edges
    has_sensor = create_edges(df, "garden_id", "sensor_id", "HAS_SENSOR")
    has_plant = create_edges(df, "garden_id", "plant_id", "HAS_PLANT")
    plant_type = create_edges(df, "plant_id", "species_id60", "PLANT_TYPE")
    measures = create_edges(df, "sensor_id", "plant_id", "MEASURES")
    located_in = create_edges(df, "garden_id", "city", "LOCATED_IN")
    predicted_for = df.select("prediction_date", "temperature_2m_max",
                                "temperature_2m_min", "rain_sum", "showers_sum", "snowfall_sum",
                                "precipitation_probability_max", "city")\
        .distinct()\
        .withColumn(":START_ID", concat(lit("TOMORROW"), monotonically_increasing_id()))\
        .withColumn(":END_ID", col("city"))\
        .withColumn(":TYPE", lit("PREDICTED_FOR"))\
        .select(":START_ID", ":END_ID", ":TYPE")

    forecasted_date = df.select('prediction_date', 'temperature_2m_max',
                                'temperature_2m_min', 'rain_sum', 'showers_sum', 'snowfall_sum',
                                'precipitation_probability_max')\
        .distinct()\
        .withColumn(":START_ID", monotonically_increasing_id())\
        .withColumn(":END_ID", col("prediction_date"))\
        .withColumn(":TYPE", lit("FORECASTED_DATE"))\
        .select(":START_ID", ":END_ID", ":TYPE")

    registers = df.select(sensor_data_cols)\
        .distinct()\
        .withColumn(":START_ID", col("sensor_id"))\
        .withColumn(":END_ID", monotonically_increasing_id())\
        .withColumn(":TYPE", lit("REGISTERS"))

    measure_date = create_edges(registers, ':END_ID', 'today', "TODAY_DATE")

    contains = [create_edges(registers, 'sensor_id', sensor_col, "CONTAINS")
                for sensor_col in sensor_data_cols if sensor_col not in ["today", "sensor_id"]]

    described_by = [create_edges(df, 'species_id60', plant_col, "DESCRIBED BY")
                    for plant_col in plant_data_cols]

    registers = registers.select(":START_ID", ":END_ID", ":TYPE")

    edge_dfs = [has_plant, plant_type, measures, located_in, predicted_for, forecasted_date, registers, measure_date]
    edge_dfs.extend(contains)
    edge_dfs.extend(described_by)

    edges = has_sensor
    for edge_df in edge_dfs:
        edges = edges.unionByName(edge_df)

    edges = edges.withColumn(":ID", monotonically_increasing_id())

    spark.stop()


if __name__ == "__main__":
    # The S3 URL and file path are passed as command-line arguments
    s3_master_url = sys.argv[1]
    s3_file_url = sys.argv[2]
    file_path = sys.argv[3]
    aws_access_key_id = sys.argv[4]
    aws_secret_access_key = sys.argv[5]
    transform(s3_master_url, s3_file_url, file_path, aws_access_key_id, aws_secret_access_key)
