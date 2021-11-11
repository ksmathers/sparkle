from .ios import Ios,Output
from typing import Dict,Callable
from pyspark.sql.session import SparkSession
import inspect

class Transform:
    def __init__(self, callback : Callable, _output : Output, _tf_type = 'simple', **ios : Dict[str,Ios]):
        self.ios = ios
        self.output = _output
        self.callback = callback
        self._tf_type = _tf_type

    def invoke(self, spark : SparkSession):
        args={}

        for n in self.ios:
            io = self.ios[n]
            if io.iodir == 'input':
                print(f"Reading {io.fpath}")
                args[n] = io.read_df(spark)
            elif isinstance(io, Output):
                args[n] = io

        cb = self.callback
        arglist = list(inspect.signature(cb).parameters.keys())
        if arglist[0] == 'ctx':
            # transform_df with ctx 
            df = self.callback(spark.sparkContext,**args)
        else:
            # transform_df without ctx
            df = self.callback(**args)

        if self._tf_type == 'simple':
            self.output._write_df(df)

