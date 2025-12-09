from .ios import Ios,Output,Input, TransformInput, TransformOutput
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
        # Prepare transform arguments
        args = {}
        for n in self.ios:
            io = self.ios[n]
            if io.iodir == 'input':
                input_wrapper = TransformInput(io.fpath, runtime)
                if self._tf_type == 'simple':
                    print(f"Reading {io.fpath}")
                    args[n] = input_wrapper.dataframe()
                else:
                    args[n] = input_wrapper
            elif io.iodir == 'output':
                output_wrapper = TransformOutput(io.fpath, runtime)
                if self._tf_type == 'simple':
                    raise RuntimeError("Simple transforms can have only input Ios")
                else:
                    args[n] = output_wrapper
            else:
                raise RuntimeError(f"Unknown iodir: {io.iodir}")

        # Invoke the transform callback
        cb = self.callback
        arglist = list(inspect.signature(cb).parameters.keys())
        if 'ctx' in arglist:
            # transform with ctx
            df = cb(ctx=TransformContext(runtime), **args)
        else:
            # transform without ctx
            df = cb(**args)

        # Write output for simple transforms.  Standard transforms handle output internally.
        if self._tf_type == 'simple':
            output_wrapper = TransformOutput(self.output.fpath, runtime)
            output_wrapper.write_dataframe(df)