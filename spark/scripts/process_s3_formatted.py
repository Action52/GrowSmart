import sys
import uuid

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit, to_date, unix_timestamp, current_date, col, concat
from pyspark.sql import DataFrame
import logging
from datetime import date, datetime
from pyspark.sql.functions import udf, round, pandas_udf
from pyspark.sql.types import StringType, DoubleType
from uuid import uuid5


@udf(returnType=StringType())
def generate_uuid(beginning, *cols):
    hashed = str(uuid5(uuid.NAMESPACE_DNS, "-".join([col_name for col_name in cols])))
    stringed = f"{beginning}_{hashed}"
    return stringed


def create_garden_nodes_df(df: DataFrame):
    gardens = df.select("iot_garden_id", "iot_garden_name", "weather_city", "iot_plant_id", "iot_sensor_id")\
        .distinct()\
        .withColumn(":ID", concat(lit("GARDEN_"), col("iot_garden_id")))\
        .withColumn(":LABEL", lit("Garden"))
    return gardens


def create_sensor_nodes_df(df: DataFrame):
    sensors = df.select("iot_sensor_id", "iot_garden_id", "iot_plant_id", "event_id")\
        .distinct()\
        .withColumn(":ID", concat(lit("SENSOR_"), col("iot_sensor_id")))\
        .withColumn(":LABEL", lit("Sensor"))
    return sensors


def create_sensor_data_nodes_df(df: DataFrame, sensor_data_cols=[]):
    if not sensor_data_cols:
        sensor_data_cols = ["light", "motion", "co", "humidity", 'smoke', 'temp', 'lpg',
                            "soil_ph", "rainfall", "soil_temp", "soil_humidity", "soil_nitrogen",
                            "soil_potassium", "soil_phosporous"]
    sensor_data_cols = [f"iot_{sens}" for sens in sensor_data_cols]
    all_cols = []
    all_cols.extend(["iot_sensor_id", "iot_garden_id", "iot_plant_id", "event_id"])
    all_cols.extend(sensor_data_cols)
    sensor_data = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", generate_uuid(lit("SENSORDATA"), col("event_id"), col("iot_sensor_id")))\
        .withColumn(":LABEL", lit("SensorData"))
    return sensor_data


def create_event_nodes_df(df: DataFrame):
    all_cols = []
    all_cols.extend(["event_id", "iot_sensor_id", "today"])
    events = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(lit("EVENT_"), col("event_id")))\
        .withColumn(":LABEL", lit("Event"))
    return events


def create_plant_nodes_df(df: DataFrame):
    plants = df.select("iot_plant_id", "iot_garden_id", "iot_species_id", "iot_sensor_id")\
        .distinct()\
        .withColumn(":ID", concat(lit("PLANT_"), col("iot_plant_id")))\
        .withColumn(":LABEL", lit("Plant"))
    return plants


def create_species_nodes_df(df: DataFrame):
    all_cols = ["iot_plant_id", "iot_species_id"]
    species = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(lit("SPECIES_"), col("iot_species_id")))\
        .withColumn(":LABEL", lit("Species"))
    return species


def create_location_nodes_df(df: DataFrame):
    locations = df.select("weather_city", "iot_garden_id")\
        .distinct()\
        .withColumn(":ID", concat(lit("LOCATION_"), col("weather_city")))\
        .withColumn(":LABEL", lit("Location"))
    return locations


def create_weather_nodes_df(df: DataFrame, weather_cols=[]):
    if not weather_cols:
        weather_cols = ['temperature_2m_max', 'temperature_2m_min', 'rain_sum',
                        'showers_sum', 'snowfall_sum', 'precipitation_probability_max']
    weather_cols = [f"weather_{weath}" for weath in weather_cols]
    all_cols = ["weather_city", "today", "weather_prediction_date"]
    all_cols.extend(weather_cols)
    weathers = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", generate_uuid(lit("WEATHER"), col("weather_city"), col("today")))\
        .withColumn(":LABEL", lit("WeatherForecast"))
    return weathers


def create_date_nodes_df(df: DataFrame):
    dates_predicted = df.select("weather_prediction_date", "event_id")\
        .withColumn(":ID", col("weather_prediction_date"))\
        .withColumn(":LABEL", lit("Date"))\
        .select(":ID", ":LABEL")
    dates_today = df.select("today", "event_id")\
        .withColumn(":ID", col("today"))\
        .withColumn(":LABEL", lit("Date")) \
        .select(":ID", ":LABEL")
    dates = dates_predicted.union(dates_today).distinct()
    return dates


def create_plant_characteristics_nodes_df(df: DataFrame, plant_data_cols=[]):
    if not plant_data_cols:
        plant_data_cols = ["Species_name_standardized_against_TPL", "Taxonomic_level",
                           "Status_according_to_TPL", "Genus", "Phylogenetic_Group_within_angiosperms",
                           "Phylogenetic_Group_General", "Adaptation_to_terrestrial_or_aquatic_habitats",
                           "Woodiness", "Growth_Form", "Succulence", "Nutrition_type_parasitism",
                           "Nutrition_type_carnivory", "Leaf_type", "Leaf_area_mm2", "Leaf_area_n_o",
                           "Plant_height_m", "Plant_height_n_o"]
    plant_data_cols = [f"plants_{plant}" for plant in plant_data_cols]
    all_cols = ["iot_species_id"]
    all_cols.extend(plant_data_cols)
    plant_data = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(lit("SPECIESDATA_"), col("iot_species_id")))\
        .withColumn(":LABEL", lit("SpeciesData"))
    return plant_data


def create_described_by_edge(species_df: DataFrame, plant_characteristics_df: DataFrame):
    # Perform an inner join on species_id
    join_condition = species_df['iot_species_id'] == plant_characteristics_df['iot_species_id']
    df = species_df.join(plant_characteristics_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(species_df[':ID'].alias(':START_ID'),
                      plant_characteristics_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('DESCRIBED_BY'))\
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))
    return edges


def create_plant_type_edge(plant_df: DataFrame, species_df: DataFrame):
    # Perform an inner join on species_id
    join_condition = plant_df['iot_species_id'] == species_df['iot_species_id']
    df = plant_df.join(species_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(plant_df[':ID'].alias(':START_ID'),
                      species_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('PLANT_TYPE')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))
    return edges


def create_measures_edge(plant_df: DataFrame, sensor_data_df: DataFrame):
    # Perform an inner join on iot_plant_id
    join_condition = plant_df['iot_plant_id'] == sensor_data_df['iot_plant_id']
    df = plant_df.join(sensor_data_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(sensor_data_df[':ID'].alias(':START_ID'),
                      plant_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('MEASURES')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_has_plant_edge(garden_df: DataFrame, plant_df: DataFrame):
    # Perform an inner join on iot_garden_id
    join_condition = garden_df['iot_garden_id'] == plant_df['iot_garden_id']
    df = garden_df.join(plant_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      plant_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('HAS_PLANT')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_has_sensor_edge(garden_df: DataFrame, sensor_df: DataFrame):
    # Perform an inner join on iot_garden_id
    join_condition = garden_df['iot_garden_id'] == sensor_df['iot_garden_id']
    df = garden_df.join(sensor_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      sensor_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('HAS_SENSOR')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_registers_edge(sensor_df: DataFrame, event_df: DataFrame):
    # Perform an inner join on event_id
    join_condition = sensor_df['event_id'] == event_df['event_id']
    df = sensor_df.join(event_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(sensor_df[':ID'].alias(':START_ID'),
                      event_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('REGISTERS')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_located_in_edge(garden_df: DataFrame, location_df: DataFrame):
    # Perform an inner join on iot_garden_id
    join_condition = garden_df['iot_garden_id'] == location_df['iot_garden_id']
    df = garden_df.join(location_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      location_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('LOCATED_IN')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_predicted_for_edge(weather_df: DataFrame, location_df: DataFrame):
    # Perform an inner join on weather_city
    join_condition = weather_df['weather_city'] == location_df['weather_city']
    df = weather_df.join(location_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(weather_df[':ID'].alias(':START_ID'),
                      location_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('PREDICTED_FOR')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_tomorrow_date_edge(weather_df: DataFrame, date_df: DataFrame):
    # Perform an inner join on weather_prediction_date
    join_condition = weather_df['weather_prediction_date'] == date_df[':ID']
    df = weather_df.join(date_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(weather_df[':ID'].alias(':START_ID'),
                      date_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('TOMORROW_DATE')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_today_date_edge(event_df: DataFrame, date_df: DataFrame):
    # Perform an inner join on today
    join_condition = event_df['today'] == date_df[':ID']
    df = event_df.join(date_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(event_df[':ID'].alias(':START_ID'),
                      date_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('TODAY_DATE')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def create_contains_edge(event_df: DataFrame, sensor_data_df: DataFrame):
    # Perform an inner join on event_id
    join_condition = event_df['event_id'] == sensor_data_df['event_id']
    df = event_df.join(sensor_data_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(event_df[':ID'].alias(':START_ID'),
                      sensor_data_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('CONTAINS')) \
        .withColumn(':ID', generate_uuid(lit("E"), col(":START_ID"), col(":END_ID")))

    return edges


def round_float_cols(df):
    for col_name in df.schema:
        if isinstance(col_name.dataType, DoubleType):
            df = df.withColumn(col_name.name, round(col(col_name.name), 3))
    return df


def transform(s3_master_url, s3_url, file_path, aws_access_key_id, aws_secret_access_key, mode="INFO"):
    conf = {
        "spark.jars": "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar",
        "spark.jars.packages": "net.java.dev.jets3t:jets3t:0.9.3",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key,
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4",
        "fs.s3a.endpoint": "s3.eu-west-3.amazonaws.com",
        "spark.driver.memory": "2G",
        #"spark.driver.host": "airflow-airflow-worker-1",
        "spark.executor.memory": "1200M",
        "spark.executor.memoryOverhead": "800M",
        #"spark.network.timeout": "500s",
        #"spark.executor.heartbeatInterval": "480s"
    }

    spark = SparkSession.builder.appName("readS3")

    for key, value in conf.items():
        spark = spark.config(key, value)

    spark = spark.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # The S3 file path is the S3 URL concatenated with the file path
    s3_file_path = f'{s3_url}/{file_path}'
    print("S3 FILE PATH: ", s3_file_path)
    df = (
        spark.read.parquet(s3_file_path, header=True, inferSchema=False)
        #.withColumnRenamed("iot_species_id60", "iot_species_id")
        .withColumn("weather_prediction_date", col("weather_prediction_date"))\
        .withColumn("today", current_date().cast(StringType()))\
        .withColumn("event_id", generate_uuid(
            lit(""), col("iot_sensor_id"), col("today"), col("iot_plant_id")))
    )

    df = round_float_cols(df)
    df.printSchema() # Check

    # Create the nodes dataframes
    garden_nodes_df = create_garden_nodes_df(df)
    sensor_nodes_df = create_sensor_nodes_df(df)
    sensor_data_nodes_df = create_sensor_data_nodes_df(df)
    event_nodes_df = create_event_nodes_df(df)
    plant_nodes_df = create_plant_nodes_df(df)
    species_nodes_df = create_species_nodes_df(df)
    location_nodes_df = create_location_nodes_df(df)
    weather_nodes_df = create_weather_nodes_df(df)
    date_nodes_df = create_date_nodes_df(df)
    plant_characteristics_nodes_df = create_plant_characteristics_nodes_df(df)

    print("Successfully created node dfs")
    #
    # Create the edges dataframes
    described_by_edge_df = create_described_by_edge(species_nodes_df, plant_characteristics_nodes_df)
    plant_type_edge_df = create_plant_type_edge(plant_nodes_df, species_nodes_df)
    measures_edge_df = create_measures_edge(plant_nodes_df, sensor_data_nodes_df)
    has_plant_edge_df = create_has_plant_edge(garden_nodes_df, plant_nodes_df)
    has_sensor_edge_df = create_has_sensor_edge(garden_nodes_df, sensor_nodes_df)
    registers_edge_df = create_registers_edge(sensor_nodes_df, event_nodes_df)
    located_in_edge_df = create_located_in_edge(garden_nodes_df, location_nodes_df)
    predicted_for_edge_df = create_predicted_for_edge(weather_nodes_df, location_nodes_df)
    tomorrow_date_edge_df = create_tomorrow_date_edge(weather_nodes_df, date_nodes_df)
    today_date_edge_df = create_today_date_edge(event_nodes_df, date_nodes_df)
    contains_edge_df = create_contains_edge(event_nodes_df, sensor_data_nodes_df)

    print("Successfully created edges dfs")
    #
    # Drop the unnecessary columns that were used to join
    garden_nodes_df = garden_nodes_df.drop("weather_city", "iot_plant_id", "iot_sensor_id").distinct()
    sensor_nodes_df = sensor_nodes_df.drop("iot_garden_id", "iot_plant_id", "event_id").distinct()
    sensor_data_nodes_df = sensor_data_nodes_df.drop("iot_sensor_id", "iot_garden_id", "iot_plant_id", "event_id").distinct()
    event_nodes_df = event_nodes_df.drop("iot_sensor_id", "today").distinct()
    plant_nodes_df = plant_nodes_df.drop("iot_garden_id", "iot_species_id", "iot_sensor_id").distinct()
    species_nodes_df = species_nodes_df.drop("iot_plant_id").distinct()
    location_nodes_df = location_nodes_df.drop("iot_garden_id").distinct()
    weather_nodes_df = weather_nodes_df.drop("weather_city", "today", "weather_prediction_date").distinct()
    date_nodes_df = date_nodes_df
    plant_characteristics_nodes_df = plant_characteristics_nodes_df.drop("iot_species_id").distinct()

    print("Successfully dropped duplicates")

    today_day = datetime.now().strftime('%Y-%m-%d')

    # Save the nodes dataframes on s3
    for node_df, name in zip(
            [garden_nodes_df, sensor_nodes_df, sensor_data_nodes_df, event_nodes_df, plant_nodes_df,
             species_nodes_df, location_nodes_df, weather_nodes_df, date_nodes_df,
             plant_characteristics_nodes_df],
            ['garden_nodes', 'sensor_nodes', 'sensor_data_nodes', 'event_nodes', 'plant_nodes',
             'species_nodes', 'location_nodes', 'weather_nodes', 'date_nodes',
             'plant_characteristics_nodes']):
        print(f"Saving {name}")
        node_df.show(5)
        node_df.write.csv(f'{s3_url}/nodes/{today_day}/{name}.csv', header=True, mode='overwrite')
        # node_df.checkpoint()

    print("Successfully saved nodes")


    # spark.catalog.clearCache()
    # Save the edges dataframes on s3
    for edge_df, name in zip([described_by_edge_df, plant_type_edge_df, measures_edge_df, has_plant_edge_df,
                         has_sensor_edge_df, registers_edge_df, located_in_edge_df, predicted_for_edge_df,
                         tomorrow_date_edge_df, today_date_edge_df, contains_edge_df],
                        ['described_by_edges', 'plant_type_edges', 'measures_edges', 'has_plant_edges',
                         'has_sensor_edges', 'registers_edges', 'located_in_edges', 'predicted_for_edges',
                         'tomorrow_date_edges', 'today_date_edges', 'contains_edges']):
        print(f"Saving {name}")
        edge_df.show(5)
        edge_df.write.csv(f'{s3_url}/edges/{today_day}/{name}.csv', mode='overwrite', header=True)
        # edge_df.checkpoint()

    print("Successfully saved edges")
    spark.stop()


if __name__ == "__main__":
    # The S3 URL and file path are passed as command-line arguments
    s3_master_url = sys.argv[1]
    s3_file_url = sys.argv[2]
    file_path = sys.argv[3]
    aws_access_key_id = sys.argv[4]
    aws_secret_access_key = sys.argv[5]
    transform(s3_master_url, s3_file_url, file_path, aws_access_key_id, aws_secret_access_key)
