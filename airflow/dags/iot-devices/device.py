import boto3
import datetime
import json
import random
import pandas as pd
import os

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
    # Upload it to s3
    uploader = S3()
    s3_session = uploader.create_s3_session()
    s3_session.Object('temporarydevicedata', key).put(Body=json.dumps(data))
