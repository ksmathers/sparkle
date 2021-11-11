from os import stat_result
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

class Ios:
    def __init__(self, fpath, iodir):
        self.iodir = iodir
        self.fpath = fpath

class Input(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'input')

    def read_df(self, spark : SparkSession):
        return spark.read.format('csv').option('header',True).load(self.fpath)

class Output(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'output')
    
    def _write_df(self, df : DataFrame):
        print(f"Saving {self.fpath} as CSV")
        df.write.format('csv').option('header',True).mode('overwrite').save(self.fpath)

    def write_dataframe(self, df : DataFrame):
        self._write_df(df)