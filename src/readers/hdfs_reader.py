class HDFSReader:
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def read_table(self, table_name, layer):
        path = f"{self.base_path}/{layer}/{table_name}"

        df = self.spark.read.format("parquet") \
            .load(path)

        return df