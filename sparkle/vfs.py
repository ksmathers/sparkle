from pyspark.sql import DataFrame
#from runtime import SparkleRuntime

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
        for k,v in self.mounts:
            if fpath.startswith(k):
                fpath.replace(k, v)
                return fpath
        raise RuntimeError("Undefined VFS mount: ", fpath)

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
        if self.format == "csv":
            df = spark.read.format('csv').option('header',True).load(vpath)
        elif self.format == "parquet":
            df = spark.read.format('parquet').load(vpath)
        else:
            raise NotImplementedError(f"Unknown format: {format}")
        return df
