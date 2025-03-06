from pathlib import Path
import logging
from src.column_store import ColumnStore
from src.data_processor import DataProcessor
from src.compressor import Compressor
from src.indexer import Indexer

# Set up logger for easy debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Create compressor and indexer here and "lend" it to data processor
compressor = Compressor()
indexer = Indexer()

# Process data by compressing the town column and indexing month column
path_to_csv = Path("data") / "ResalePricesSingapore.csv"
data_processor = DataProcessor(compressor, indexer, logger)
data_processor.load_data(path_to_csv)
data_processor.process_data()
processed_df = data_processor.get_df()

# Create the different column stores
column_store = ColumnStore(logger)
column_store.create_column_stores("data/storage", processed_df)


# FYI section to log out the compressing decoder & indexing map
print("This is to show the compressor data")
print(data_processor.compressor.column_decoders)
print(data_processor.compressor.column_dictionaries)

print("This is to show the indexing outcome")
print(indexer.get_index("month", "2015-06"))

