from os import stat_result
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

    def _read_df(self, runtime) -> DataFrame:
        return runtime.vfs.read_df(self.fpath, self.format)

    def dataframe(self) -> DataFrame:
        from .runtime import SparkleRuntime
        return self._read_df(SparkleRuntime.instance())

class Output(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'output')

    def _write_df(self, df : DataFrame, runtime):
        runtime.vfs.write_df(df, self.fpath, self.format)

    def write_dataframe(self, df : DataFrame):
        from .runtime import SparkleRuntime
        self._write_df(df, SparkleRuntime.instance())