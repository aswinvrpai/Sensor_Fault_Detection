import csv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dataclasses import dataclass
from dotenv import load_dotenv
import sys
import pandas as pd
load_dotenv()

# This command is very important for running the code;
# Below command is used to modify the Python path at runtime, specifically to include a parent directory to the module search path
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../../')
from src.logger import logging
from src.utils import MongoDbConnect
from src.exception import CustomException
sys.path.pop(1)

@dataclass
class DataIngestionConfig:
    raw_file_path:str=os.path.join('artifacts','raw.csv')
    test_file_path:str=os.path.join('artifacts','test.csv')
    train_file_path:str=os.path.join('artifacts','train.csv')

class DataIngestion:
    
    def __init__(self):
        self.data_ingestion_config=DataIngestionConfig()
    
    def initiate_data_ingestion(self):
        
        try:
            
            # Log
            logging.info('Retrieving data from MongoDB Client and writing to raw.csv')
            
            # Retrieve Data from MongoDb;
            MONGO_DB_URL=os.getenv('MONGO_DB_URL')
            client=MongoDbConnect(MONGO_DB_URL)
            
            # Dataframe;
            df=client.retrieve_data_from_mongodb('wafer_sensor_data','collection1')
            os.makedirs(os.path.dirname(self.data_ingestion_config.raw_file_path),exist_ok=True)
            raw_csv=self.data_ingestion_config.raw_file_path
            df.to_csv(raw_csv,index=False)
            
            # Log
            logging.info('Retrieving data from MongoDB Client and writing to raw.csv completed')
            
        except Exception as e:
            logging.info('Data ingestion error occurred')
            raise CustomException(e,sys)
        
        pass
    
if __name__ == "__main__":
    
    ingestion_obj=DataIngestion()
    ingestion_obj.initiate_data_ingestion()