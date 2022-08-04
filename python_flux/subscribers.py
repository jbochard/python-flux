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
        return value, self.context


class SForeach(SSubscribe):
    def __init__(self, on_success, on_error, ctx, f):
        super(SForeach, self).__init__(ctx, f)
        self.on_success = on_success
        self.on_error = on_error

    def __next__(self):
        try:
            value, ctx = super(SForeach, self).__next__()
            self.on_success(value)
            return value
        except Exception as e:
            self.on_error(e)
            raise e
