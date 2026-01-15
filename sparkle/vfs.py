import glob
import json
import os
from typing import Optional, Callable
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
    def __init__(self, runtime, mounts={}, mangle_paths: Optional[Callable]=None, debug=False, active_branch='develop', backup_branch='master'):
        self.mounts = mounts
        self.runtime = runtime
        self.runtime.register_vfs(self)
        self.mangle_paths = mangle_paths
        self.debug = debug
        self.active_branch = active_branch
        self.backup_branch = backup_branch

    def add_mounts(self, mounts):
        """
        mounts :Dict[str,str]: Dictionary of vfs prefixes to actual locations
            e.g. { '/MyProject': 'C:\\Users\\Kevin\\git\\sparkle' }
        """
        for k,v in mounts.items():
            self.mounts[k] = v

    def vfspath(self, fpath, mode, active_branch='master', backup_branch='master') -> str:
        if self.debug:
            print(f"VFS resolving path: {fpath} (mode={mode})")
        if self.mangle_paths is not None:
            fpath = self.mangle_paths(fpath)
        for k,v in self.mounts.items():
            if fpath.startswith(k):
                return fpath.replace(k, v)
        raise RuntimeError("Undefined VFS mount: ", fpath)
    
    def vfstype(self, fpath : str, mode : str, active_branch='master', backup_branch='master') -> str:
        local_path = self.vfspath(fpath, 'r', active_branch, backup_branch)  # Validate VFS path
        if mode == 'r':
            files = glob.glob(local_path + "/*")
            if any(f.endswith('.parquet') or f.endswith('.pq') for f in files):
                ftype = "parquet"
            elif any(f.endswith('.csv') for f in files):
                ftype = "csv"
            else:
                raise RuntimeError(f"Could not auto-detect input format for path: {self.fpath}")
        elif mode == 'w':
            # Default to Parquet for output.  
            ftype = "parquet"
        else:
            raise RuntimeError(f"Unknown mode: {mode}")
        return ftype
    
    def filesystem(self, fpath : str, read_only=False):
        vpath = self.vfspath(fpath, 'w', self.active_branch, self.backup_branch)
        if not os.path.isdir(vpath):
            os.makedirs(vpath)
        return FileSystem(vpath, read_only)

    def write_df(self, df : DataFrame, fpath : str, ftype : Optional[str] = None):
        vpath = self.vfspath(fpath, 'w', self.active_branch, self.backup_branch)
        print(f"Saving {vpath} as {ftype}")
        if ftype is None:
            ftype = self.vfstype(fpath, 'w', self.active_branch, self.backup_branch)
        if ftype == "csv":
            df.write.format('csv').option('header',True).mode('overwrite').save(vpath)
        else:
            df.write.format('parquet').mode('overwrite').save(vpath)

    def load_schema(self, fpath : str) -> Optional[T.StructType]:
        vpath = os.path.join(self.vfspath(fpath, 'r', self.active_branch, self.backup_branch), "_/schema.json")
        if not os.path.exists(vpath):
            raise RuntimeError(f"{os.path.basename(fpath)}: Schema file not found at {vpath}")
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

    def read_df(self, fpath : str, ftype : Optional[str] = None):
        vpath = self.vfspath(fpath, 'r', self.active_branch, self.backup_branch)
        print(f"Loading {vpath} as {ftype}")
        spark = self.runtime.spark
        if ftype is None:
            ftype = self.vfstype(fpath, 'r', self.active_branch, self.backup_branch)
        if ftype == "csv":
            schema = self.load_schema(fpath)
            df = spark.read.format('csv').option('header',True).schema(schema).load(vpath)
        elif ftype == "parquet":
            df = spark.read.format('parquet').load(vpath)
        else:
            raise NotImplementedError(f"Unknown format: {ftype}")
        return df

class VfsDataset:
    def __init__(self, vfs : Vfs, basepath : str):
        self.vfs = vfs
        self.basepath = vfs.vfspath(basepath, 'w', vfs.active_branch, vfs.backup_branch)

    def open(self, fpath, mode='r'):
        return open(os.path.join(self.basepath, fpath), mode)
    