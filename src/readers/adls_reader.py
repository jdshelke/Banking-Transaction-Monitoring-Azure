class ADLSReader:
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def read_table(self, table_name, layer):
        path = f"{self.base_path}/{layer}/{table_name}"
        schema_path = f"{self.base_path}/autoloader/schema/{table_name}"

        df = self.spark.readStream.format("cloudFiles") \
                  .option("cloudFiles.format", "parquet") \
                  .option("cloudFiles.schemaLocation", schema_path) \
                  .load(path)

        return df