from pyspark.sql.functions import current_timestamp
from src.sql.create_gold_tables import createGoldTables


class GoldWriter:

    def __init__(self, spark):
        self.spark = spark

    def write_delta_table(self, df, table_name, layer):

        full_table_name = f"banking.{layer}.{table_name}"

        # Add Gold audit column
        df = df.withColumn(
            "gold_ingestion_time",
            current_timestamp()
        )

        # Create Gold table
        createGoldTables(self.spark, table_name)

        df.printSchema()
        
        # Overwrite Gold table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)
        
        print(f"Refresh completed: {full_table_name}")


