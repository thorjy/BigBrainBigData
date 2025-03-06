import pandas as pd
from .compressor import Compressor
from .indexer import Indexer
import logging

class DataProcessor:
    def __init__(self, compressor: Compressor, indexer: Indexer, logger : logging.Logger):
        self.logger = logger
        self.compressor = compressor
        self.indexer = indexer
        self.df = None
        self.key_columns = ["month", "town", "floor_area_sqm", "resale_price"]  # Define the key columns

    def load_data(self, csv_file):
        """Load data from the CSV file into the DataFrame and extract key columns"""
        self.df = pd.read_csv(csv_file, usecols=self.key_columns)  # Only load the key columns
        self.df.fillna('NULL', inplace=True)  # Replace missing values with 'NULL'
        self.logger.info(f"Data loaded successfully. Filled missing values with 'NULL'.")

    def process_data(self):
        """Compress and index the data"""
        # Compress the 'town' column
        self.df['town'] = self.df['town'].apply(lambda town: self.compressor.compress("town", town))  # Apply compression
        self.logger.info("Town data compressed.")

        # Indexing by month column by "year-month" as keys
        self.indexer.create_index(self.df, "month")
        self.logger.info("Done indexing month column")

    def get_df(self):
        return self.df
    
    # decode also can do here