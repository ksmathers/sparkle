from .ios import Ios,Output,Input
from typing import Dict,Callable
import inspect


class TransformContext:
    def __init__(self, runtime, is_incremental = False):
        self.runtime = runtime
        self.is_incremental = is_incremental

class Transform:
    def __init__(self, callback : Callable, _output : Output, _tf_type = 'simple', **ios : Dict[str,Ios]):
        self.ios = ios
        self.output = _output
        self.callback = callback
        self._tf_type = _tf_type

    def invoke(self, runtime):
        args={}
        for n in self.ios:
            io = self.ios[n]
            if io.iodir == 'input' and self._tf_type == 'simple':
                print(f"Reading {io.fpath}")
                args[n] = io._read_df(runtime)
            else:
                args[n] = io

        cb = self.callback
        arglist = list(inspect.signature(cb).parameters.keys())
        if 'ctx' in arglist:
            # transform with ctx
            df = cb(ctx=TransformContext(runtime), **args)
        else:
            # transform without ctx
            df = cb(**args)

        if self._tf_type == 'simple':
            self.output._write_df(df, runtime)

