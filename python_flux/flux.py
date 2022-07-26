import time

from jsonmerge import merge

from python_flux.subscribers import SSubscribe


class Flux(object):

    def filter(self, f):
        return FFilter(f, self)

    def map(self, f):
        return FMap(f, self)

    def map_context(self, f):
        return FMapContext(f, self)

    def flat_map(self, f):
        return FFlatMap(f, self)

    def do_on_next(self, f):
        return FDoOnNext(f, self)

    def delay(self, d):
        return FDelay(d, self)

    def take(self, n):
        return FTake(n, self)

    def log(self, log=lambda v: str(v)):
        return FLog(log, self)

    def log_context(self, log=lambda c: str(c)):
        return FLogContext(log, self)

    def subscribe(self, context={}):
        return SSubscribe(context, self)


class Stream(Flux):
    def __init__(self, up):
        super(Stream, self).__init__()
        self.upstream = up

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        return value, ctx


class FFilter(Stream):
    def __init__(self, p, flux):
        super().__init__(flux)
        self.predicate = p

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None or not self.predicate(value):
            value, ctx = self.upstream.next(context)
        return value, ctx


class FTake(Stream):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.idx = 0

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        self.idx = self.idx + 1
        if self.idx <= self.count:
            return value, ctx
        else:
            raise StopIteration()


class FDelay(Stream):
    def __init__(self, delay, flux):
        super().__init__(flux)
        self.delay = delay

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        time.sleep(self.delay)
        return value, ctx


class FLog(Stream):
    def __init__(self, log, flux):
        super().__init__(flux)
        self.function_log = log

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
            print(f"{str(self.function_log(value))}", flush=True)
        return value, ctx


class FLogContext(Stream):
    def __init__(self, log, flux):
        super().__init__(flux)
        self.function_log = log

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        print(f"{str(self.function_log(ctx))}", flush=True)
        return value, ctx


class FMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        return self.function(value, ctx), ctx


class FMapContext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        return value, merge(ctx, self.function(value, ctx))


class FFlatMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func
        self.current = None

    def next(self, context):
        ctx = context
        while True:
            while self.current is None:
                value, ctx = self.upstream.next(context)
                while value is None:
                    value, ctx = self.upstream.next(context)
                self.current = self.function(value, ctx).subscribe(ctx)
            try:
                v = next(self.current)
                while v is None:
                    v = next(self.current)
                return v, ctx
            except StopIteration as si:
                self.current = None


class FDoOnNext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = self.upstream.next(context)
        while value is None:
            value, ctx = self.upstream.next(context)
        self.function(value, ctx)
        return value, ctx