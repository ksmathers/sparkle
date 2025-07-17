from .ios import Ios,Output,Input
from typing import Dict,Callable,cast
import inspect


class TransformContext:
    def __init__(self, runtime, is_incremental = False):
        self.auth_header = None
        self.fallback_branches = []
        self.parameters = {}
        self.spark_session = runtime.spark
        self.runtime = runtime
        self.is_incremental = is_incremental

    def abort_job(self, msg):
        raise Exception(msg)


class Transform:
    def __init__(self, callback : Callable, _output : Output, _tf_type = 'simple', **ios : Ios):
        self.ios : Dict[str, Ios] = ios
        self.output = _output
        self.callback = callback
        self._tf_type = _tf_type
        self._profile = []
        self._allowed_run_duration = None
        self._run_as_user = False
        self._is_incremental = False
        self._require_incremental = False
        self._semantic_version = 1
        self._snapshot_inputs = []
        self._allow_retention = False
        self._strict_append = False
        self._v2_semantics = False

    def invoke(self, runtime):
        args={}
        for n in self.ios:
            io = self.ios[n]
            if io.iodir == 'input' and self._tf_type == 'simple':
                print(f"Reading {io.fpath}")
                args[n] = cast(Input, io)._read_df(runtime)
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

