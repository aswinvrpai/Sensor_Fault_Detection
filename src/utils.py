import csv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dataclasses import dataclass
from dotenv import load_dotenv
import sys
import pandas as pd
from src.logger import logging
from src.exception import CustomException
import numpy as np
import pickle

@dataclass
class MongoDbConnect:
    def __init__(self,url) -> None:
        
        logging.info('Connecting to MongoDB & Client intialized')
        
        # Create a new client and connect to the server
        self.client = MongoClient(url, server_api=ServerApi('1'))
    
    """Upload data from CSV to MongoDb"""
    def upload_data_to_mongodb(self,csv_file,db_name,collection_name):

        try:
            db=self.client[db_name]
            collection=db[collection_name]
            
            # Log;
            logging.info('Uploading CSV to MongoDB Client')

            with open(csv_file,'r') as fopen:
                reader=csv.DictReader(fopen)
                
                for row in reader:
                    collection.insert_one(row)
                    
            if(collection.count_documents({}) != 0):
                logging.info('Uploading CSV to MongoDB Client Done')
                
        except Exception as e:
            logging.info('Error occurred in Upload Data to MongoDb')
            raise CustomException(e,sys)
    
    """Retrieve data from MongoDb and return as Pandas dataframe"""
    def retrieve_data_from_mongodb(self,db_name,collection_name):
        
        try:
            
            # Log
            logging.info('Retrieving data from MongoDB Client')
            
            # Get Data from Collection;
            mongo_db_data=list(self.client[db_name][collection_name].find())
            
            # Datframe;
            df=pd.DataFrame(mongo_db_data)
            
            # Drop the column name '_id'
            df.drop(columns='_id', inplace=True)
            
            df.replace({'na': np.nan}, inplace=True)
            
            # Log
            logging.info('Retrieving data from MongoDB Client completed')
            
            return df
            
        except Exception as e:
            logging.info('Error while Retrieving data from MongoDb')
            raise CustomException(e,sys)

"""Function to save data object to a pickle file"""
def save_object(data_object,file_path):
    
    try:
        
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        
        with open(file_path,'wb') as fobj:
            pickle.dump(data_object,fobj)
            
    except Exception as e:
        logging.info(f'Save Object to Pickle File {file_path} failed.')
        raise CustomException(e,sys)

"""Function to load data object from a pickle file"""       
def load_object(file_path):
    try:
        with open(file_path,'rb') as file_obj:
            return pickle.load(file_obj)
    except Exception as e:
        logging.info('Load Object from Pickle File {file_path} failed.')
        raise CustomException(e,sys)
        
if __name__ == "__main__":
    
    load_dotenv()
    
    MONGO_DB_URL=os.getenv('MONGO_DB_URL')
    client=MongoDbConnect(MONGO_DB_URL)

    # client.upload_data_to_mongodb(r'C:\Work_Directory\Learn\DS_Projects\Sensor_Fault_Detection\notebooks\data\test.csv','wafer_sensor_data','collection1')
    
    df=client.retrieve_data_from_mongodb('wafer_sensor_data','collection1')
    print(df)
        
        