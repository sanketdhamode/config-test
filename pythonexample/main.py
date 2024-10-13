import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import os

# Load configuration from YAML
def load_config(config_file='config.yaml'):
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise Exception(f"Configuration file '{config_file}' not found.")
    except yaml.YAMLError as e:
        raise Exception(f"Error parsing YAML configuration file: {e}")

config = load_config()

# Database Configuration
DB_CONFIG = config['database']
ETL_CONFIG = config['etl']

# Extract tables information
tables_info = config['tables']

# Abstract base class for data fetching
class DataFetcher(ABC):
    @abstractmethod
    def fetch_data(self, table_name: str, sql_file: str, offset: int, page_size: int, value: str):
        pass

# SQL Server Data Fetcher Implementation with parameterized queries
class SQLServerDataFetcher(DataFetcher):
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection_str = (
            f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@"
            f"{db_config['server']}/{db_config['database']}?"
            f"driver={db_config['driver']}"
        )
        self.engine = create_engine(self.connection_str)
        
    def _load_sql(self, sql_file: str) -> str:
        if not os.path.isfile(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        with open(sql_file, 'r') as file:
            return file.read()
    
    def fetch_data(self, table_name: str, sql_file: str, offset: int, page_size: int, value: str) -> pd.DataFrame:
        raw_query = self._load_sql(sql_file)
        query = text(raw_query)
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn, params={"value": value, "offset": offset, "page_size": page_size})
        except Exception as e:
            raise Exception(f"Error fetching data from table '{table_name}': {e}")

# Abstract base class for writing data to file
class DataWriter(ABC):
    @abstractmethod
    def write_data(self, df: pd.DataFrame, file_path: str):
        pass

# Parquet File Writer Implementation using PyArrow
class ParquetFileWriter(DataWriter):
    def write_data(self, df: pd.DataFrame, file_path: str):
        try:
            table = pa.Table.from_pandas(df)
            with pq.ParquetWriter(file_path, table.schema, compression='snappy') as writer:
                writer.write_table(table)
        except Exception as e:
            raise Exception(f"Error writing data to Parquet file '{file_path}': {e}")

# Main ETL Processor
class ETLProcessor:
    def __init__(self, data_fetcher: DataFetcher, data_writer: DataWriter, page_size: int):
        self.data_fetcher = data_fetcher
        self.data_writer = data_writer
        self.page_size = page_size
        
    def process_table(self, table_name: str, sql_file: str, output_dir: str, value: str):
        offset = 0
        more_data = True
        while more_data:
            try:
                df = self.data_fetcher.fetch_data(table_name, sql_file, offset, self.page_size, value)
                if df.empty:
                    more_data = False
                else:
                    file_path = f"{output_dir}/{table_name}.parquet"
                    self.data_writer.write_data(df, file_path)
                    offset += self.page_size
            except Exception as e:
                print(f"Error processing table '{table_name}': {e}")
                break  # Exit the loop on error

    def process_tables(self, tables_info: list, output_dir: str, value: str, thread_pool_size: int):
        with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
            futures = [
                executor.submit(self.process_table, table['name'], table['sql_file'], output_dir, value)
                for table in tables_info
            ]
            for future in futures:
                try:
                    future.result()  # Wait for the thread to complete and check for exceptions
                except Exception as e:
                    print(f"Error in processing table: {e}")

# Execution Script
if __name__ == "__main__":
    try:
        output_dir = "output_path"  # Set your output directory
        value = "2024-07-28"  # Example date value for entrydate

        # Instantiate data fetcher and writer
        fetcher = SQLServerDataFetcher(DB_CONFIG)
        writer = ParquetFileWriter()
        
        # Create ETL processor and execute
        etl_processor = ETLProcessor(fetcher, writer, page_size=ETL_CONFIG['page_size'])
        etl_processor.process_tables(tables_info, output_dir, value, thread_pool_size=ETL_CONFIG['thread_pool_size'])

        print("ETL process completed successfully.")
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
