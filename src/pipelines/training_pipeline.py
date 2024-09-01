import os
import pandas as pd
import sys

# Data Ingestion code;
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../../')
from src.components.data_ingestion import DataIngestion
sys.path.pop(1)


if __name__ == "__main__":
    
    # Data Ingestion
    ingestion_obj=DataIngestion()
    train_file,test_file=ingestion_obj.initiate_data_ingestion()