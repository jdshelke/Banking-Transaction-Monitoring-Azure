from pyspark.sql import SparkSession
from src.utils import config_reader
from src.jobs.ingestion_job import BronzeIngestion

if __name__ == "__main__":
    config = config_reader.load_config("config/config.yaml")

    spark = SparkSession.builder.appName("BankingTransactionMonitoring") \
                                .master("local[*]") \
                                .getOrCreate()

    mysql_config = config["mysql"]
    hdfs_config = config["hdfs"]

    bronze_ingestion = BronzeIngestion(spark, mysql_config, hdfs_config)

    bronze_ingestion.ingestion("loans")
    bronze_ingestion.ingestion("support_tickets")
    bronze_ingestion.ingestion("transactions")
    bronze_ingestion.ingestion("customers")
    bronze_ingestion.ingestion("employees")
    bronze_ingestion.ingestion("loan_payments")
    bronze_ingestion.ingestion("cards")
    bronze_ingestion.ingestion("card_transactions")
    bronze_ingestion.ingestion("accounts")
    bronze_ingestion.ingestion("branches")