from pyspark.sql import SparkSession
from pyspark.sql.functions import col, months_between, floor

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-GoldJob") \
        .master("local[*]") \
        .getOrCreate()

    # start_date = "2018-03-13 00:00:00.0"
    # end_date = "2023-01-19 00:00:00.0"

    data = [("2023-01-19 00:00:00.0", "2018-03-13 00:00:00.0")]
    df = spark.createDataFrame(data, ["end_timestamp", "start_timestamp"])

    df_months = df.withColumn(
            "fractional_months", 
            months_between(col("end_timestamp"), col("start_timestamp"))
        ).withColumn(
            "full_months", 
            floor(months_between(col("end_timestamp"), col("start_timestamp")))
        )

    df_months.show()