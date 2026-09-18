class DeltaTableReader:
    def __init__(self, spark):
        self.spark = spark

    def read_table(self, table_name, layer):
        full_table_name = f"banking.{layer}.{table_name}"

        df = self.spark.readStream.table(full_table_name)

        return df