from pyspark.sql import DataFrame
#from runtime import SparkleRuntime
import os

class FileSystem:
    def __init__(self, basepath, read_only=False):
        self.basepath = basepath
        self.readonly = read_only

    def open(self, fpath, mode='r', **kwargs):
        if self.readonly and 'w' in mode:
            raise RuntimeError("Filesystem is read-only")
        fullpath = os.path.join(self.basepath, fpath)
        return open(fullpath, mode, **kwargs)

    def files(self, glob=None, regex=".*", show_hidden=False) -> DataFrame:
        raise NotImplementedError("Not implemented for non-local filesystems")
    
    def ls(self, glob=None, regex=".*", show_hidden=False) -> list:
        raise NotImplementedError("Not implemented for non-local filesystems")


class Vfs:
    def __init__(self, runtime, mounts={}):
        self.mounts = mounts
        self.runtime = runtime
        self.runtime.register_vfs(self)

    def add_mounts(self, mounts):
        """
        mounts :Dict[str,str]: Dictionary of vfs prefixes to actual locations
            e.g. { '/MyProject': 'C:\\Users\\Kevin\\git\\sparkle' }
        """
        for k,v in mounts.items():
            self.mounts[k] = v

    def vfspath(self, fpath):
        for k,v in self.mounts.items():
            if fpath.startswith(k):
                return fpath.replace(k, v)
        raise RuntimeError("Undefined VFS mount: ", fpath)
    
    def filesystem(self, fpath : str, read_only=False):
        vpath = self.vfspath(fpath)
        if not os.path.isdir(vpath):
            os.makedirs(vpath)
        return FileSystem(vpath, read_only)

    def write_df(self, df : DataFrame, fpath : str, format : str):
        vpath = self.vfspath(fpath)
        print(f"Saving {vpath} as {format}")
        if format == "csv":
            df.write.format('csv').option('header',True).mode('overwrite').save(vpath)
        else:
            df.write.format('parquet').mode('overwrite').save(vpath)

    def read_df(self, fpath : str, format : str):
        vpath = self.vfspath(fpath)
        print(f"Loading {vpath} as {format}")
        spark = self.runtime.spark
        if format == "csv":
            df = spark.read.format('csv').option('header',True).load(vpath)
        elif format == "parquet":
            df = spark.read.format('parquet').load(vpath)
        else:
            raise NotImplementedError(f"Unknown format: {format}")
        return df
