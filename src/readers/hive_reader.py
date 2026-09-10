class HiveReader:
    def __init__(self, spark, database):
        self.spark = spark
        self.database = database
        
    def read_table(self, table_name, database=None):

        database = database or self.database

        df = self.spark.table(f"{database}.{table_name}")

        return df