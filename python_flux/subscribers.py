import traceback


class SSubscribe(object):
    def __init__(self, ctx, f):
        if type(ctx) == dict:
            self.context = ctx
        else:
            self.context = ctx()
        self.flux = f

    def __gen(self, context):
        for value, ctx in self.flux._next(context):
            if value is not None:
                yield value, ctx

    def __iter__(self):
        return iter(map(lambda t: t[0], iter(self.__gen(self.context))))

    def foreach(self, on_success=lambda v, c: print(v), on_error=lambda e, c: print(e)):
        try:
            for value, ctx in self.__gen(self.context):
                on_success(value, ctx)
        except Exception as e:
            on_error(e, ctx)
            traceback.print_exc()

