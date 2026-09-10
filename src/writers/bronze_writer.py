class BronzeWriter:
    def __init__(self, namenode, base_path):
        self.namenode = namenode
        self.base_path = base_path

    def write_parquet(self, df, table_name, layer):
        df.write \
            .format("parquet") \
            .mode("append") \
            .save(f"{self.namenode}{self.base_path}/{layer}/{table_name}")
