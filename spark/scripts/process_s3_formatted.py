import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit, to_date, unix_timestamp, current_date, col, concat
from pyspark.sql import DataFrame
import logging
from datetime import date


def create_garden_nodes_df(df: DataFrame):
    gardens = df.select("garden_id", "garden_name", "city", "plant_id", "sensor_id")\
        .distinct()\
        .withColumn(":ID", col("garden_id"))\
        .withColumn(":LABEL", lit("Garden"))
    return gardens


def create_sensor_nodes_df(df: DataFrame):
    sensors = df.select("sensor_id", "garden_id", "plant_id", "event_id")\
        .distinct()\
        .withColumn(":ID", col("sensor_id"))\
        .withColumn(":LABEL", lit("Sensor"))
    return sensors


def create_sensor_data_nodes_df(df: DataFrame, sensor_data_cols=[]):
    if not sensor_data_cols:
        sensor_data_cols = ["light", "motion", "co", "humidity", 'smoke', 'temp', 'lpg',
                            "soil_ph", "rainfall", "soil_temp", "soil_humidity", "soil_nitrogen",
                            "soil_potassium", "soil_phosporous"]
    all_cols = []
    all_cols.extend(["sensor_id", "garden_id", "plant_id", "event_id"])
    all_cols.extend(sensor_data_cols)
    sensor_data = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(col("event_id"), monotonically_increasing_id()))\
        .withColumn(":LABEL", lit("SensorData"))
    return sensor_data


def create_event_nodes_df(df: DataFrame):
    all_cols = []
    all_cols.extend(["event_id", "sensor_id", "today"])
    events = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", col("event_id"))\
        .withColumn(":LABEL", lit("Event"))
    return events


def create_plant_nodes_df(df: DataFrame):
    plants = df.select("plant_id", "garden_id", "species_id60", "sensor_id")\
        .distinct()\
        .withColumn(":ID", col("plant_id"))\
        .withColumn(":LABEL", lit("Plant"))
    return plants


def create_species_nodes_df(df: DataFrame):
    all_cols = ["plant_id", "species_id60"]
    species = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", col("species_id60"))\
        .withColumn(":LABEL", lit("Species"))
    return species


def create_location_nodes_df(df:DataFrame):
    locations = df.select("city", "garden_id")\
        .distinct()\
        .withColumn(":ID", col("city"))\
        .withColumn(":LABEL", lit("Location"))
    return locations


def create_weather_nodes_df(df: DataFrame, weather_cols=[]):
    if not weather_cols:
        weather_cols = ['temperature_2m_max', 'temperature_2m_min', 'rain_sum',
                        'showers_sum', 'snowfall_sum', 'precipitation_probability_max']
    all_cols = ["city", "today", "prediction_date"]
    all_cols.extend(weather_cols)
    weathers = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(col("city"), col("today"), monotonically_increasing_id()))\
        .withColumn(":LABEL", lit("WeatherForecast"))
    return weathers


def create_date_nodes_df(df: DataFrame):
    dates_predicted = df.select("prediction_date", "event_id")\
        .withColumn(":ID", col("prediction_date"))\
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
    all_cols = ["species_id60"]
    all_cols.extend(plant_data_cols)
    plant_data = df.select(all_cols)\
        .distinct()\
        .withColumn(":ID", concat(col("species_id60"), monotonically_increasing_id()))\
        .withColumn(":LABEL", lit("SpeciesData"))
    return plant_data


def create_described_by_edge(species_df: DataFrame, plant_characteristics_df: DataFrame):
    # Perform an inner join on species_id60
    join_condition = species_df['species_id60'] == plant_characteristics_df['species_id60']
    df = species_df.join(plant_characteristics_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(species_df['species_id60'].alias(':START_ID'),
                      plant_characteristics_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('DESCRIBED_BY'))
    return edges


def create_plant_type_edge(plant_df: DataFrame, species_df: DataFrame):
    # Alias the dataframes
    plant_df_alias = plant_df.alias('plant_df')
    species_df_alias = species_df.alias('species_df')

    # Perform an inner join on species_id60
    join_condition = plant_df_alias['species_id60'] == species_df_alias['species_id60']
    df = plant_df_alias.join(species_df_alias, join_condition, 'inner')

    # Define the edges
    edges = df.select(df['plant_df.plant_id'].alias(':START_ID'),
                      df['species_df.species_id60'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('PLANT_TYPE'))
    return edges



def create_measures_edge(plant_df: DataFrame, sensor_data_df: DataFrame):
    # Perform an inner join on plant_id
    join_condition = plant_df['plant_id'] == sensor_data_df['plant_id']
    df = plant_df.join(sensor_data_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(sensor_data_df[':ID'].alias(':START_ID'),
                      plant_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('MEASURES'))

    return edges


def create_has_plant_edge(garden_df: DataFrame, plant_df: DataFrame):
    # Perform an inner join on garden_id
    join_condition = garden_df['garden_id'] == plant_df['garden_id']
    df = garden_df.join(plant_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      plant_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('HAS_PLANT'))

    return edges


def create_has_sensor_edge(garden_df: DataFrame, sensor_df: DataFrame):
    # Perform an inner join on garden_id
    join_condition = garden_df['garden_id'] == sensor_df['garden_id']
    df = garden_df.join(sensor_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      sensor_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('HAS_SENSOR'))

    return edges


def create_registers_edge(sensor_df: DataFrame, event_df: DataFrame):
    # Perform an inner join on event_id
    join_condition = sensor_df['event_id'] == event_df['event_id']
    df = sensor_df.join(event_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(sensor_df[':ID'].alias(':START_ID'),
                      event_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('REGISTERS'))

    return edges


def create_located_in_edge(garden_df: DataFrame, location_df: DataFrame):
    # Perform an inner join on garden_id
    join_condition = garden_df['garden_id'] == location_df['garden_id']
    df = garden_df.join(location_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(garden_df[':ID'].alias(':START_ID'),
                      location_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('LOCATED_IN'))

    return edges


def create_predicted_for_edge(weather_df: DataFrame, location_df: DataFrame):
    # Perform an inner join on city
    join_condition = weather_df['city'] == location_df['city']
    df = weather_df.join(location_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(weather_df[':ID'].alias(':START_ID'),
                      location_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('PREDICTED_FOR'))

    return edges


def create_tomorrow_date_edge(weather_df: DataFrame, date_df: DataFrame):
    # Perform an inner join on prediction_date
    join_condition = weather_df['prediction_date'] == date_df[':ID']
    df = weather_df.join(date_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(weather_df[':ID'].alias(':START_ID'),
                      date_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('TOMORROW_DATE'))

    return edges


def create_today_date_edge(event_df: DataFrame, date_df: DataFrame):
    # Perform an inner join on today
    join_condition = event_df['today'] == date_df[':ID']
    df = event_df.join(date_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(event_df[':ID'].alias(':START_ID'),
                      date_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('TODAY_DATE'))

    return edges


def create_contains_edge(event_df: DataFrame, sensor_data_df: DataFrame):
    # Perform an inner join on event_id
    join_condition = event_df['event_id'] == sensor_data_df['event_id']
    df = event_df.join(sensor_data_df, join_condition, 'inner')

    # Define the edges
    edges = df.select(event_df[':ID'].alias(':START_ID'),
                      sensor_data_df[':ID'].alias(':END_ID')) \
        .distinct() \
        .withColumn(':TYPE', lit('CONTAINS'))

    return edges


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
        #.withColumnRenamed("species_id6060", "species_id60")
        .withColumn("prediction_date", to_date(unix_timestamp("prediction_date", "dd/MM/yy").cast("timestamp")))
        .withColumn("today", lit(current_date()))
        .withColumn("event_id", concat(col("sensor_id"), col("today"), monotonically_increasing_id()))
    )

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

    # Drop the unnecessary columns that were used to join
    garden_nodes_df = garden_nodes_df.drop("city", "plant_id", "sensor_id").distinct()
    sensor_nodes_df = sensor_nodes_df.drop("garden_id", "plant_id", "event_id").distinct()
    sensor_data_nodes_df = sensor_data_nodes_df.drop("sensor_id", "garden_id", "plant_id", "event_id").distinct()
    event_nodes_df = event_nodes_df.drop("sensor_id", "today").distinct()
    plant_nodes_df = plant_nodes_df.drop("garden_id", "species_id60", "sensor_id").distinct()
    species_nodes_df = species_nodes_df.drop("plant_id").distinct()
    location_nodes_df = location_nodes_df.drop("garden_id").distinct()
    weather_nodes_df = weather_nodes_df.drop("city", "today", "prediction_date").distinct()
    date_nodes_df = date_nodes_df
    plant_characteristics_nodes_df = plant_characteristics_nodes_df.drop("species_id60").distinct()

    # Save the nodes dataframes on s3
    for df, name in zip([garden_nodes_df, sensor_nodes_df, sensor_data_nodes_df, event_nodes_df, plant_nodes_df,
                         species_nodes_df, location_nodes_df, weather_nodes_df, date_nodes_df,
                         plant_characteristics_nodes_df],
                        ['garden_nodes', 'sensor_nodes', 'sensor_data_nodes', 'event_nodes', 'plant_nodes',
                         'species_nodes', 'location_nodes', 'weather_nodes', 'date_nodes',
                         'plant_characteristics_nodes']):
        df.coalesce(1).write.csv(f'{s3_url}/nodes/{name}', mode='overwrite', header=True)

    # Save the edges dataframes on s3
    for df, name in zip([described_by_edge_df, plant_type_edge_df, measures_edge_df, has_plant_edge_df,
                         has_sensor_edge_df, registers_edge_df, located_in_edge_df, predicted_for_edge_df,
                         tomorrow_date_edge_df, today_date_edge_df, contains_edge_df],
                        ['described_by_edges', 'plant_type_edges', 'measures_edges', 'has_plant_edges',
                         'has_sensor_edges', 'registers_edges', 'located_in_edges', 'predicted_for_edges',
                         'tomorrow_date_edges', 'today_date_edges', 'contains_edges']):
        df.coalesce(1).write.csv(f'{s3_url}/edges/{name}', mode='overwrite', header=True)

    spark.stop()


if __name__ == "__main__":
    # The S3 URL and file path are passed as command-line arguments
    s3_master_url = sys.argv[1]
    s3_file_url = sys.argv[2]
    file_path = sys.argv[3]
    aws_access_key_id = sys.argv[4]
    aws_secret_access_key = sys.argv[5]
    transform(s3_master_url, s3_file_url, file_path, aws_access_key_id, aws_secret_access_key)
