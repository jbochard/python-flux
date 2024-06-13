from jsonmerge import merge


class SSubscribe(object):
    def __init__(self, on_error, on_empty, ctx, f):
        if type(ctx) == dict:
            self.context = ctx
        else:
            self.context = ctx()
        self.flux = f
        self.on_error = on_error
        self.on_empty = on_empty

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            self.flux.prepare_next()
            value, ctx = self.flux.next(self.context)
            if value.is_stop():
                raise StopIteration()
            if value.is_error() and self.on_error == 'SKIP':
                self.context = merge(self.context, ctx)
                continue
            if value.is_error() and self.on_error == 'THROW':
                raise value.err()
            if value.is_error() and self.on_error == 'BREAK':
                raise StopIteration()
            if value.is_empty() and self.on_empty == 'SKIP':
                self.context = merge(self.context, ctx)
                continue
            if value.is_empty() and self.on_empty == 'NONE':
                return None
            self.context = merge(self.context, ctx)
            return value.val()
