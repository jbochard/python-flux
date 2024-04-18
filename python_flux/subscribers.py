from functools import partial

from jsonmerge import merge
from python_flux.flux_utilis import FluxUtils as fu


class SSubscribe(object):
    def __init__(self, skip_error, ctx, f):
        if type(ctx) == dict:
            self.context = ctx
        else:
            self.context = ctx()
        self.flux = f
        self.skip_error = skip_error

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            self.flux.prepare_next()
            value, e, ctx = self.flux.next(self.context)
            if isinstance(e, StopIteration):
                raise e
            if e is not None and self.skip_error:
                self.context = merge(self.context, ctx)
                continue
            if e is not None:
                raise e
            self.context = merge(self.context, ctx)
            return value


class SForeach(object):
    def __init__(self, on_success, on_error, on_finish, ctx, f):
        self.on_success = on_success
        self.on_error = on_error
        self.on_finish = on_finish
        self.context = ctx
        self.flux = f

    def __next__(self):
        while True:
            self.flux.prepare_next()
            value, e, ctx = self.flux.next(self.context)
            if e is not None and isinstance(e, StopIteration):
                fu.try_or(partial(self.on_finish), ctx)
                raise e
            elif e is not None:
                fu.try_or(partial(self.on_error), e, ctx)
            else:
                fu.try_or(partial(self.on_success), value, ctx)
            self.context = merge(self.context, ctx)
            return value
