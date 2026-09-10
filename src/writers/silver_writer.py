from pyspark.sql.functions import current_timestamp
from src.sql.create_silver_tables import createSilverTables


class SilverWriter:

    def __init__(self, spark):
        self.spark = spark

    def write_hive_table(self, df, table_name):

        full_table_name = f"silver.{table_name}"

        # Add Silver audit column
        df = df.withColumn(
            "silver_ingestion_time",
            current_timestamp()
        )

        # Create Silver table
        createSilverTables(self.spark, table_name)
            
        # Overwrite Silver table
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable(full_table_name)
        
        print(f"Upsert completed: {full_table_name}")


