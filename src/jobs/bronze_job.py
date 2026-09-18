from pyspark.sql import SparkSession

import argparse
from src.utils import config_reader
from src.readers.adls_reader import ADLSReader
from src.writers.bronze_writer import BronzeWriter

class BronzeIngestion:
    def __init__(self, spark, adls_config):
        self.spark = spark
        self.adls_config = adls_config

        self.adls_reader = ADLSReader(self.spark, self.adls_config["base_path"])
        self.bronze_writer = BronzeWriter(self.adls_config["base_path"])

    def ingestion(self, table_name):
        landing_df = self.adls_reader.read_table(table_name, "landing")

        self.bronze_writer.write_delta(landing_df, table_name, "bronze")

if __name__ == "__main__":

    config = config_reader.load_config(
        "config/config.yaml"
    )

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-BronzeIngestion") \
        .getOrCreate()

    adls_config = config["adls"]

    bronze_ingestion = BronzeIngestion(
        spark,
        adls_config
    )

    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True, help="Source table to ingest")

    args = parser.parse_args()

    bronze_ingestion.ingestion(args.table)

    spark.stop()