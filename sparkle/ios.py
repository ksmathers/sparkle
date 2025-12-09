import glob
from os import stat_result
from typing import Any
from pyspark.sql import DataFrame

class Ios:
    def __init__(self, fpath : str, iodir: str):
        """
        An IO specification for Sparkle datasets.
        
        :param fpath: Source or destination directory path for the dataset.  Contents will be CSV or Parquet files.
        :type fpath: str
        :param iodir: 'input' or 'output' to specify direction of data flow.
        :type iodir: str
        """
        self.iodir = iodir
        self.fpath = fpath

    def _auto_format(self, runtime) -> str:
        local_path = runtime.vfs.vfspath(self.fpath)  # Validate VFS path
        if self.iodir == 'input':
            files = glob.glob(local_path + "/*")
            if any(f.endswith('.parquet') or f.endswith('.pq') for f in files):
                format = "parquet"
            elif any(f.endswith('.csv') for f in files):
                format = "csv"
            else:
                raise RuntimeError(f"Could not auto-detect input format for path: {self.fpath}")
        elif self.iodir == 'output':
            # Default to Parquet for output.  
            format = "parquet"
        else:
            raise RuntimeError(f"Unknown iodir: {self.iodir}")
        return format

    def _filesystem(self, read_only, runtime):
        return runtime.vfs.filesystem(self.fpath, read_only=read_only)

class Input(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'input')

    def _read_df(self, runtime) -> DataFrame:
        return runtime.vfs.read_df(self.fpath, self._auto_format(runtime))
     

class TransformInput(Input):
    def __init__(self, fpath : str, runtime : Any):
        super().__init__(fpath)
        from .runtime import SparkleRuntime
        assert(isinstance(runtime, SparkleRuntime))
        self.runtime = runtime

    def dataframe(self) -> DataFrame:
        return self._read_df(self.runtime)
    
    def filesystem(self):
        return self._filesystem(read_only=True, runtime=self.runtime)


class Output(Ios):
    def __init__(self, fpath : str):
        super().__init__(fpath,'output')

    def _write_df(self, df : DataFrame, runtime):
        runtime.vfs.write_df(df, self.fpath, 'parquet')


class TransformOutput(Output):
    def __init__(self, fpath : str, runtime : Any):
        super().__init__(fpath)
        from .runtime import SparkleRuntime
        assert(isinstance(runtime, SparkleRuntime))
        self.runtime = runtime

    def write_dataframe(self, df : DataFrame):
        self._write_df(df, self.runtime)

    def filesystem(self):
        return self._filesystem(read_only=False, runtime=self.runtime)
        