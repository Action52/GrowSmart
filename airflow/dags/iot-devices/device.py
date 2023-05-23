import boto3
import datetime
import json
import random
import pandas as pd
import os

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()

class Device():
    data = {}
    DEVICES_PATH = './data/devices.csv'
    CROPS_PATH = './data/crops.csv'
    
    def __init__(self) -> None:
        pass
    
    def _get_data(self, path):
        return pd.read_csv(path)
    
    def _build_data(self, devices, crops):
        self.data = {
            "device_id": str(devices['device']),
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
            "soil_phosporous": float(crops['P'])
            
        }
        return self.data
    
    def create_data(self):
        '''
            Simulate iot data by shuffling data from csv file
        '''
        crops = self._get_data(self.CROPS_PATH)
        devices = self._get_data(self.DEVICES_PATH)
        
        crops_size = len(crops)
        devices_size = len(devices)
        
        return self._build_data(
            devices.loc[random.randrange(1, devices_size-1)], 
                    crops.loc[random.randrange(1, crops_size-1)]
            )


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
    # Create random data
    d = Device()
    data = d.create_data()
    
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"{data['device_id']}_{timestamp}"
    
    # # # Upload it to s3
    uploader = S3()
    s3_session = uploader.create_s3_session()
    s3_session.Object('temporarydevicedata', key).put(Body=json.dumps(data))

    # Stream to Kafka
    broker = KafkaBroker()
    broker.send_message(topic_name="iot_devices_data", message_key=key, message=json.dumps(data))
