import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd

from sklearn.model_selection import train_test_split
from dataclasses import dataclass

# Easy to define just the variables within a class with the dataclass decorator
@dataclass
class DataIngestionConfig:
    '''defining the paths for data storage'''
    train_data_path: str = os.path.join('artifacts', "train.csv")
    test_data_path: str = os.path.join('artifacts', "test.csv")
    raw_data_path: str = os.path.join('artifacts', "data.csv")

# if you are defining other functions in a class, then it is better to use the init function
class DataIngestion:
    '''Main data ingestion function'''
    def __init__(self):
        '''Initializing the function with the 3 data paths in the ingestion_config object'''
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        '''If your data is stored in some database, this function will read the data from the source and split it into train and test sets'''
        logging.info("Entered the data ingestion method or component")
        try:
            # data can be read from any source (SQL, mongoDB, etc)
            # Here I am reading it from a csv in my data folder
            df = pd.read_csv('notebook/data/stud.csv') 
            logging.info("Read the data set as a dataframe")
            
            #ensuring that the directories for the three paths exist, otherwise the following code creates them
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok = True)
            os.makedirs(os.path.dirname(self.ingestion_config.test_data_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)

            # ensures that the raw data is saved in the raw_data_path as backup before manipulation
            df.to_csv(self.ingestion_config.raw_data_path, index = False, header = True)

            logging.info("Train Test Split Initiated")
            # A particular value of the random_state will ensure that the splitting of the data is deterministic 
            # ... with repeated execution of the code
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)
            
            #index false tells the command not to store the indexes
            # header true tells the command to store the header row in the first row 
            train_set.to_csv(self.ingestion_config.train_data_path, index = False, header = True)
            test_set.to_csv(self.ingestion_config.test_data_path, index = False, header = True)

            logging.info("Ingestion of the data is complete")
            
            return (
                # paths are required for the data transformation operations
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            raise CustomException(e, sys) # raises custom exception with the error message e and sys as the system information
        
#block is only executed when the script is run directly, and not when it is imported as a module in another script. 
# This helps prevent unintended side effects when the script is imported.
if __name__ == '__main__':
    obj=DataIngestion()
    obj.initiate_data_ingestion()