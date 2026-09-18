class BronzeWriter:
    def __init__(self, base_path):
        self.base_path = base_path

    def write_delta(self, df, table_name, layer):
        checkpoint_path = f"{self.base_path}/autoloader/checkpoint/{layer}/{table_name}"
        write_path = f"{self.base_path}/{layer}/{table_name}"
        bronze_table = f"banking.{layer}.{table_name}"
        
        df.printSchema()

        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(once=True) \
            .toTable(bronze_table)
        
        query.awaitTermination()
