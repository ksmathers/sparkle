#import sys
import os
import inspect
#sys.path.insert(0, '/usr/local/spark/python')
#sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9-src.zip')
from pyspark.sql import SparkSession

class SparkleRuntime:
    def __init__(self):
        self.spark = None
        self.transforms = []

    def start(self, appname="sparkle", driver_memory="1g", executor_memory="4g"):
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} --executor-memory {executor_memory} pyspark-shell"
        self.spark = SparkSession.builder.appName(appname).getOrCreate()

    
    def add_transform(self, tf):
        self.transforms.append(tf)

    def submit(self):
        if self.spark is None:
            self.start()
        for tf in self.transforms:
            args = inspect.getargspec(tf)
            params = None
            tf(self.spark.sparkContext, )