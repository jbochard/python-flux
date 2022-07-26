import traceback

from jsonmerge import merge


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
        value, ctx = self.flux.next(self.context)
        self.context = merge(self.context, ctx)
        return value

    def __default_success(v):
        pass

    def __default_error(e):
        traceback.print_exception(e)

    def foreach(self, on_success=__default_success, on_error=__default_error):
        try:
            for value in self:
                on_success(value)
        except Exception as e:
            on_error(e)

