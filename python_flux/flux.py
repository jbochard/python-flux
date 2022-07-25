import time

from python_flux.subscribers import SSubscribe


class Flux(object):
    def __init__(self):
        super(Flux, self).__init__()

    def filter(self, f):
        return FFilter(f, self)

    def map(self, f):
        return FMap(f, self)

    def map_context(self, f):
        return FMapContext(f, self)

    def flat_map(self, f):
        return FFlatMap(f, self)

    def delay(self, d):
        return FDelay(d, self)

    def take(self, n):
        return FTake(n, self)

    def log(self, log=lambda v: str(v)):
        return FLog(log, self)

    def log_context(self, log=lambda c: str(c)):
        return FLogContext(log, self)

    def subscribe(self, on_success=lambda v: print(str(v)), on_error=lambda e: print(f"Exception: {e}"), context={}):
        return SSubscribe(on_success, on_error, context, self)


class Stream(Flux):
    def __init__(self, up):
        super(Stream, self).__init__()
        self.upstream = up

    def _map_value_gen(self, value, context):
        yield value

    def _map_context_gen(self, value, context):
        yield context

    def _next(self, context):
        for value, ctx in self.upstream._next(context):
            if value is not None:
                for v in self._map_value_gen(value, ctx):
                    for c in self._map_context_gen(value, ctx):
                        ctx.update(c)
                        yield v, ctx


class FFilter(Stream):
    def __init__(self, p, flux):
        super().__init__(flux)
        self.predicate = p

    def _map_value_gen(self, value, context):
        if self.predicate(value):
            yield value


class FTake(Stream):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.idx = 0

    def _map_value_gen(self, value, context):
        self.idx = self.idx + 1
        if self.idx <= self.count:
            yield value
        else:
            return


class FDelay(Stream):
    def __init__(self, delay, flux):
        super().__init__(flux)
        self.delay = delay

    def _map_value_gen(self, value, context):
        yield value
        time.sleep(self.delay)


class FLog(Stream):
    def __init__(self, log, flux):
        super().__init__(flux)
        self.function_log = log

    def _map_value_gen(self, value, context):
        print(f"{str(self.function_log(value))}", flush=True)
        yield value


class FLogContext(Stream):
    def __init__(self, log, flux):
        super().__init__(flux)
        self.function_log = log

    def _map_context_gen(self, value, context):
        print(f"{str(self.function_log(context))}", flush=True)
        yield context


class FMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def _map_value_gen(self, value, context):
        yield self.function(value, context)


class FMapContext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def _map_context_gen(self, value, context):
        yield self.function(value, context)


class FFlatMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def _map_value_gen(self, value, context):
        gen = self.function(value, context)
        if gen is not None:
            for v in gen:
                if v is not None:
                    yield v
