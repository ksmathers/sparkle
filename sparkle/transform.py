from .ios import Ios,Output
from typing import Dict,Callable
from pyspark.sql.session import SparkSession
import inspect

class Transform:
    def __init__(self, callback : Callable, output : Output, **ios : Dict[str,Ios]):
        self.ios = ios
        self.output = output
        self.callback = callback

    def invoke(self,spark : SparkSession):
        args={}
        for n in self.ios:
            io = self.ios[n]
            if io.iodir == 'input':
                print(f"Reading {io.fpath}")
                args[n] = io.read_df(spark)

        cb = self.callback
        arglist = inspect.signature(cb).parameters.keys()
        if arglist[0] == 'ctx':
            # transform_df with ctx 
            df = self.callback(spark.sparkContext,**args)
        else:
            # transform_df without ctx
            df = self.callback(**args)
        self.output.write_df(spark,df)

