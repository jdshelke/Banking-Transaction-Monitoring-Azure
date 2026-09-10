from pyspark.sql import SparkSession

import argparse
from src.utils import config_reader
from src.readers.hdfs_reader import HDFSReader
from src.writers.silver_writer import SilverWriter

from src.transformations.silver import (
    customer_transform,
    account_transform,
    transaction_transform,
    employee_transform,
    card_transform,
    branch_transform,
    card_transaction_transform,
    loan_transform,
    loan_payment_transform,
    support_ticket_transform
)


class SilverJob:
    def __init__(self, spark, hdfs_config):
        self.spark = spark
        self.hdfs_config = hdfs_config

        self.hdfs_reader = HDFSReader(self.spark, self.hdfs_config["base_path"])
        self.silver_writer = SilverWriter(self.spark)

    def process_table(self, table_name, transform_function):
        bronze_df = self.hdfs_reader.read_table(table_name, "bronze")

        silver_df = transform_function(bronze_df)

        self.silver_writer.write_hive_table(silver_df, table_name)


if __name__ == "__main__":

    config = config_reader.load_config(
        "config/config.yaml"
    )

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-SilverJob") \
        .enableHiveSupport() \
        .getOrCreate()

    hdfs_config = config["hdfs"]

    silver_job = SilverJob(spark, hdfs_config)

    transformations = {
        "customers": customer_transform.transform,
        "accounts": account_transform.transform,
        "transactions": transaction_transform.transform,
        "employees": employee_transform.transform,
        "cards": card_transform.transform,
        "branches": branch_transform.transform,
        "card_transactions": card_transaction_transform.transform,
        "loans": loan_transform.transform,
        "loan_payments": loan_payment_transform.transform,
        "support_tickets": support_ticket_transform.transform
    }

    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True, help="Parquet files to Hive tables")

    args = parser.parse_args()

    transform_function = transformations[args.table]

    silver_job.process_table(args.table, transform_function)

    spark.stop()