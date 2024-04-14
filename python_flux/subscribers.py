from functools import partial

from jsonmerge import merge
from python_flux.flux_utilis import FluxUtils as fu


class SSubscribe(object):
    def __init__(self, ctx, f):
        if type(ctx) == dict:
            self.context = ctx
        else:
            self.context = ctx()
        self.flux = f

    def __iter__(self):
        return self

    def __next__(self):
        self.flux.prepare_next()
        value, e, ctx = self.flux.next(self.context)
        if e is not None:
            raise e
        self.context = merge(self.context, ctx)
        return value


class SForeach(SSubscribe):
    def __init__(self, on_success, on_error, ctx, f):
        super(SForeach, self).__init__(ctx, f)
        self.on_success = on_success
        self.on_error = on_error

    def __next__(self):
        while True:
            self.flux.prepare_next()
            value, e, ctx = self.flux.next(self.context)
            if e is not None and isinstance(e, StopIteration):
                raise e
            elif e is not None:
                fu.try_or(partial(self.on_error), e, ctx)
            else:
                fu.try_or(partial(self.on_success), value, ctx)
            self.context = merge(self.context, ctx)
            return value
