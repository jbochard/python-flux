from jsonmerge import merge


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
