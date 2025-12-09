import json
import os
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import types as T
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

    def load_schema(self, fpath : str) -> Optional[T.StructType]:
        schema_path = os.path.join(fpath, "schema.json")
        vpath = self.vfspath(schema_path)
        if not os.path.exists(vpath):
            raise RuntimeError(f"Schema file not found: {vpath}")
        with open(vpath, 'r') as f:
            schema_json = json.load(f)
        # Foundry schema is in the form of a List[Dict] where each Dict has 'name' and 'type' keys
        # We need to convert it to Spark StructType
        cols = []
        for col in schema_json:
            col_name = col['name']
            col_type = col['type']
            col_nullable = col.get('nullable', True)
            if col_type == 'INTEGER':
                col = T.StructField(col_name, T.IntegerType(), col_nullable)
            elif col_type == 'DOUBLE':
                col = T.StructField(col_name, T.DoubleType(), col_nullable)
            elif col_type == 'STRING':
                col = T.StructField(col_name, T.StringType(), col_nullable)
            else:
                raise NotImplementedError(f"Unknown column type: {col_type}")
            cols.append(col)
        return T.StructType(cols)

    def read_df(self, fpath : str, format : str):
        vpath = self.vfspath(fpath)
        print(f"Loading {vpath} as {format}")
        spark = self.runtime.spark
        if format == "csv":
            schema = self.load_schema(fpath)
            df = spark.read.format('csv').option('header',True).schema(schema).load(vpath)
        elif format == "parquet":
            df = spark.read.format('parquet').load(vpath)
        else:
            raise NotImplementedError(f"Unknown format: {format}")
        return df

class VfsDataset:
    def __init__(self, vfs : Vfs, basepath : str):
        self.vfs = vfs
        self.basepath = vfs.vfspath(basepath)

    def open(self, fpath, mode='r'):
        return open(os.path.join(self.basepath, fpath), mode)
    