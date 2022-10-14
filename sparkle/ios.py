from os import stat_result
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame



class Ios:
    def __init__(self, fpath, iodir):
        self.iodir = iodir
        self.fpath = fpath
        self.format = "csv"
        if fpath.endswith(".parquet") or fpath.endswith(".pq"):
            self.format = "parquet"

class Input(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'input')

    def read_df(self, spark : SparkSession):
        print(f"Loading {self.fpath} as {self.format}")
        if self.format == "csv":
            return spark.read.format('csv').option('header',True).load(self.fpath)
        elif self.format == "parquet":
            return spark.read.format('parquet').load(self.fpath)

    def dataframe(self):
        return self.read_df(SparkSession.getActiveSession())

class Output(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'output')
    
    def _write_df(self, df : DataFrame):
        print(f"Saving {self.fpath} as {self.format}")
        if self.format == "csv":
            df.write.format('csv').option('header',True).mode('overwrite').save(self.fpath)
        else:
            df.write.format('parquet').mode('overwrite').save(self.fpath)

    def write_dataframe(self, df : DataFrame):
        self._write_df(df)