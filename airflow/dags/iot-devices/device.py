import boto3
import datetime
import json
import random
import pandas as pd
import os
import uuid
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()

class Device():
    data = {}
    DEVICES_PATH = './data/devices.csv'
    CROPS_PATH = './data/crops.csv'
    SPECIES_PATH = './data/species.csv'
    PLANT_CACHE_PATH = './cache/plants.json'
    
    CONSTRAINT = {
        "Big Size Garden": 9,
        "Middle Size Garden": 4,
        "Small Garden With Lamp": 4,
    }
    
    MAPPING = [
        {'device_id': 'b8:27:eb:bf:9d:51', 'location': 'Taragona', 'garden_id': 'BC4FDD', 'garden_name': 'Middle Size Garden'},
        {'device_id': 'p0:33:18:00:20:11', 'location': 'Lleida', 'garden_id': '16FFB3', 'garden_name': 'Middle Size Garden'},
        {'device_id': 'az:12:rb:ss:34:gh', 'location': 'Girona', 'garden_id': 'B5A7D2', 'garden_name': 'Small Garden With Lamp'},
        {'device_id': '18:24:as:kf:24:00', 'location': 'Barcelona', 'garden_id': '4D2E71', 'garden_name': 'Big Size Garden'},
        {'device_id': 'kd:sd:3a:33:69:42', 'location': 'Girona', 'garden_id': '91DE92', 'garden_name': 'Big Size Garden'},
        {'device_id': 'o0:4e:ve:rt:1l:l1', 'location': 'Madrid', 'garden_id': '75DF16', 'garden_name': 'Big Size Garden'},
    ]

    MAPPING_SIZE = len(MAPPING)
    
    def __init__(self) -> None:
        pass
    
    def _get_data(self, path):
        return pd.read_csv(path)
    
    def _build_data(self, devices, crops, species):
        rand_data  = self.MAPPING[random.randrange(0, self.MAPPING_SIZE)]
        plant_cache = self._read_plant_cache()
        
        # If cache is not full generate new data else get data from cache
        if len(plant_cache[rand_data['device_id']]) < self.CONSTRAINT[rand_data['garden_name']]:
            plant_id = str(uuid.uuid4())
            species_id = str(int(species['Species_id']))
            plant_cache[rand_data['device_id']].append(f'{plant_id}:{species_id}')
            
            # Write to cache
            self._write_plant_cache(plant_cache)

        else:
            # assign random plant from cache
            plant = plant_cache[rand_data['device_id']][random.randrange(0, len(plant_cache[rand_data['device_id']]))]
            plant_id, species_id = plant.split(":")

                    
        self.data = {
            "device_id": rand_data['device_id'],
            "light": bool(devices['light']),
            "motion": bool(devices['motion']),
            "co": float(devices['co']),
            "humidity": float(devices['humidity']),
            'smoke': float(devices['smoke']),
            'temp': float(devices['temp']),
            'lpg': float(devices['lpg']),
            "soil_ph": float(crops['ph']),
            "rainfall": float(crops['rainfall']),
            "soil_temp": float(crops['temperature']),
            "soil_humidity": float(crops['humidity']),
            "soil_nitrogen": float(crops['N']),
            "soil_potassium": float(crops['K']),
            "soil_phosporous": float(crops['P']),
            "garden_id": rand_data['garden_id'],
            "garden_name": rand_data['garden_name'],
            "location": rand_data['location'],
            'species_id': species_id,
            'plant_id': plant_id
        }
        return self.data
    
    def create_data(self):
        '''
            Simulate iot data by shuffling data from csv file
        '''
        crops = self._get_data(self.CROPS_PATH)
        devices = self._get_data(self.DEVICES_PATH)
        species = self._get_data(self.SPECIES_PATH)
        
        crops_size = len(crops)
        devices_size = len(devices)
        species_size = len(species)
        
        return self._build_data(
            devices.loc[random.randrange(0, devices_size)], 
                    crops.loc[random.randrange(0, crops_size)],
                    species.loc[random.randrange(0, species_size)]
            )
    
    def _read_plant_cache(self):
        with open(self.PLANT_CACHE_PATH, "r") as file:
            data = json.load(file)
        
        return data
    
    def _write_plant_cache(self, plant_cache):
        with open(self.PLANT_CACHE_PATH, "w") as file:
            file.write(json.dumps(plant_cache))
        
class KafkaBroker():
    ### Simulate in local machine
    bootstrap_servers = 'localhost:29092'
    
    def __init__(self):
        pass
    
    def _create_topic(self, topic_name):
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
    
    def send_message(self, topic_name, message_key, message):
        self._create_topic(topic_name)
        producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        producer.produce(topic_name, key=message_key, value=message)
        producer.flush()
        
class S3(): 
    def __init__(self) -> None:
        pass
    
    def create_s3_session(self):
        session = boto3.Session(
            region_name="eu-west-3",
            aws_access_key_id=os.getenv('aws_access_key'),
            aws_secret_access_key=os.getenv('aws_secret_access_key')
        )
        
        return session.resource('s3')

if __name__ == "__main__":   
    MAX_STREAM = 100
    
    stream_count = 1
    while True and stream_count <= MAX_STREAM:
        # Create random data
        d = Device()
        data = d.create_data()
        
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        key = f"{data['device_id']}_{timestamp}"
        
        # Upload it to s3
        uploader = S3()
        s3_session = uploader.create_s3_session()
        s3_session.Object('temporarydevicedata', key).put(Body=json.dumps(data))

        # Stream to Kafka
        if os.getenv('is_kafka_enabled') == "True":
            print("Send to Kafka")
            broker = KafkaBroker()
            broker.send_message(topic_name="iot_devices_data", message_key=key, message=json.dumps(data))
        
        stream_count += 1
        time.sleep(1)
