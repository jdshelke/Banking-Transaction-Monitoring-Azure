from pyspark.sql import SparkSession

import argparse
from src.readers.hive_reader import HiveReader
from src.writers.gold_writer import GoldWriter

from src.transformations.gold import (
    customer_transaction_summary,
    account_transaction_summary,
    daily_transaction_metrics,
    loan_payment_metrics,
    card_fraud_summary,
    branch_performance,
    customer_risk_profile,
)


class GoldJob:
    def __init__(self, spark):
        self.spark = spark

        self.hive_reader = HiveReader(self.spark, "silver")
        self.gold_writer = GoldWriter(self.spark)

    def process_table(self, table_name, transform_function, required_tables):

        table_dfs = {}

        for table, database in required_tables.items():
            table_dfs[f"{table}_df"] = self.hive_reader.read_table(table, database)

        gold_df = transform_function(**table_dfs)

        self.gold_writer.write_hive_table(gold_df, table_name)


if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-GoldJob") \
        .enableHiveSupport() \
        .getOrCreate()


    gold_job = GoldJob(spark)

    transformations = {
        "customer_transaction_summary": customer_transaction_summary.create_customer_transaction_summary,
        "account_transaction_summary": account_transaction_summary.create_account_transaction_summary,
        "daily_transaction_metrics": daily_transaction_metrics.create_daily_transaction_metrics,
        "loan_payment_metrics": loan_payment_metrics.create_loan_payment_metrics,
        "card_fraud_summary": card_fraud_summary.create_card_fraud_summary,
        "branch_performance": branch_performance.create_branch_performance,
        "customer_risk_profile": customer_risk_profile.create_customer_risk_profile
    }

    required_tables_map = {
        "customer_transaction_summary": {
            "customers": "silver",
            "accounts": "silver",
            "transactions": "silver" 
        },
        "account_transaction_summary": {
            "accounts": "silver",
            "transactions": "silver"
        },
        "daily_transaction_metrics":{
            "transactions": "silver"
        },
        "loan_payment_metrics": {
            "loans": "silver",
            "loan_payments": "silver"
        },
        "card_fraud_summary": {
            "cards": "silver",
            "card_transactions": "silver"
        },
        "branch_performance": {
            "branches": "silver",
            "accounts": "silver",
            "transactions": "silver",
            "loans": "silver",
            "employees": "silver",
            "loan_payment_metrics": "gold"
        },
        "customer_risk_profile": {
            "customers": "silver",
            "customer_transaction_summary": "gold",
            "account_transaction_summary": "gold",
            "loan_payment_metrics": "gold"
        }
        
    }

    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True, help="Parquet files to Hive tables")

    args = parser.parse_args()

    transform_function = transformations[args.table]
    required_tables = required_tables_map[args.table]

    gold_job.process_table(args.table, transform_function, required_tables)

    spark.stop()