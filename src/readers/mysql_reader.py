class MySQLReader:
    def __init__(self, spark, host, port, database, username, password, driver):
        self.spark = spark
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
    def read_table(self, table_name):
        df = self.spark.read.format("jdbc") \
                       .option("url", f"jdbc:mysql://{self.host}:{self.port}/{self.database}") \
                       .option("dbtable", table_name) \
                       .option("user", self.username) \
                       .option("password", self.password) \
                       .option("driver", self.driver) \
                       .load()

        return df