import sys
import os
import inspect
#sys.path.insert(0,'/usr/local/spark/python')
#sys.path.insert(0,'/usr/local/spark/python/lib/py4j-0.10.9-src.zip')
from pyspark.sql import SparkSession
from typing import List,Optional
from .transform import Transform
from .vfs import Vfs
from .ios import Ios,Input,Output


class SparkleRuntime:
    INSTANCE = None

    def __init__(self):
        assert(SparkleRuntime.INSTANCE is None)
        self.spark : Optional[SparkSession] = None
        self.transforms : List[Transform] = []
        self.vfs : Optional[Vfs] = None
        SparkleRuntime.INSTANCE = self

    def register_vfs(self, vfs : Vfs):
        self.vfs = vfs

    def reset(self):
        self.transforms = []

    @classmethod
    def instance(cls):
        if cls.INSTANCE is None:
            cls.INSTANCE = SparkleRuntime()
        return cls.INSTANCE

    def start(self,appname="sparkle",driver_memory="1g",executor_memory="4g"):
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} --executor-memory {executor_memory} pyspark-shell"
        self.spark = SparkSession.builder.appName(appname).getOrCreate()

    def add_transform(self,tf : Transform):
        self.transforms.append(tf)

    def submit(self):
        if self.spark is None:
            print("Spark session not started.  Starting with default params.", file=sys.stderr)
            self.start()
        for tf in self.transforms:
            tf.invoke(self)

    def load(self,ios : Input):
        return ios._read_df(self)


def transform_df(output,**ios):
    assert(isinstance(output,Ios))
    for io in ios:
        assert(isinstance(ios[io],Ios))

    def transform_decorator(func):
        return Transform(func, _output=output, **ios)
    return transform_decorator

def transform(**ios):
    for io in ios:
        assert(isinstance(ios[io],Ios))
    def transform_decorator(func):
        return Transform(func, _output=None, _tf_type='full', **ios)
    return transform_decorator
