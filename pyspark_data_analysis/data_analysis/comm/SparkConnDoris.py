class SparkConnDoris:
    def __init__(self, df, table):
        self.df = df
        self.table = table

    def write_doris(self):
        self.df.write.format("doris") \
            .option("doris.table.identifier", f"private_station.{self.table}") \
            .option("doris.fenodes", "172.20.10.3:8070") \
            .option("user", "root") \
            .option("password", "") \
            .option("save_mode", "overwrite") \
            .save()
