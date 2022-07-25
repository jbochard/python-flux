import traceback


class SSubscribe(object):
    def __init__(self, on_success, on_error, ctx, f):
        self.on_success = on_success
        self.on_error = on_error
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

    def subscribe(self):
        try:
            for value, ctx in self.__gen(self.context):
                self.on_success(value, ctx)
        except Exception as e:
            self.on_error(e, ctx)
            traceback.print_exc()

