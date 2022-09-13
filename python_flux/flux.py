import datetime
import time
import traceback
import logging
from datetime import timedelta

from jsonmerge import merge

from python_flux.subscribers import SSubscribe, SForeach


class Flux(object):

    def __default_success(value):
        pass

    def __default_error(e):
        if not isinstance(e, StopIteration):
            traceback.print_exception(type(e), e, e.__traceback__)

    def filter(self, predicate):
        return FFilter(predicate, self)

    def filter_with_context(self, predicate_with_context):
        return FFilterWithContext(predicate_with_context, self)

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

    def take_during_timedelta(self, n):
        return FTakeDuringTimeDelta(n, self)

    def log(self, log=lambda v: str(v), level=logging.INFO):
        return FLog(log, level, self)

    def log_context(self, log=lambda c: str(c), level=logging.INFO):
        return FLogContext(log, level, self)

    def subscribe(self, context={}):
        return SSubscribe(context, self)

    def foreach(self, on_success=__default_success, on_error=__default_error, context={}):
        for _ in SForeach(on_success, on_error, context, self):
            pass

    def to_list(self, context={}):
        return list(map(lambda v: v[0], iter(SSubscribe(context, self))))


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
        value, ctx = super(FFilter, self).next(context)
        while not self.predicate(value):
            value, ctx = super(FFilter, self).next(context)
        return value, ctx


class FFilterWithContext(Stream):
    def __init__(self, p, flux):
        super().__init__(flux)
        self.predicate = p

    def next(self, context):
        value, ctx = super(FFilterWithContext, self).next(context)
        ctx_bkp = ctx.copy()
        while not self.predicate(value, ctx_bkp):
            value, ctx = super(FFilterWithContext, self).next(context)
        return value, ctx


class FTake(Stream):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.idx = 0

    def next(self, context):
        value, ctx = super(FTake, self).next(context)
        self.idx = self.idx + 1
        if self.idx <= self.count:
            return value, ctx
        else:
            raise StopIteration()


class FTakeDuringTimeDelta(Stream):
    def __init__(self, td, flux):
        super().__init__(flux)
        if type(td) == timedelta:
            self.timedelta = td
        if type(td) == int or type(td) == float:
            self.timedelta = datetime.timedelta(seconds=td)
        self.starttime = None

    def next(self, context):
        if self.starttime is None:
            self.starttime = datetime.datetime.utcnow()

        value, ctx = super(FTakeDuringTimeDelta, self).next(context)
        if self.starttime + self.timedelta >= datetime.datetime.utcnow():
            return value, ctx
        else:
            raise StopIteration()


class FDelay(Stream):
    def __init__(self, delay, flux):
        super().__init__(flux)
        self.delay = delay

    def next(self, context):
        value, ctx = super(FDelay, self).next(context)
        time.sleep(self.delay)
        return value, ctx


class FLog(Stream):
    def __init__(self, log, level, flux):
        super().__init__(flux)
        self.level = level
        self.function_log = log

    def log(self, msg):
        if self.level is logging.ERROR:
            logging.error(str(msg))
        elif self.level is logging.WARNING or self.level is logging.WARN:
            logging.warning(str(msg))
        elif self.level is logging.INFO:
            logging.info(str(msg))
        elif self.level is logging.DEBUG:
            logging.debug(str(msg))

    def next(self, context):
        value, ctx = super(FLog, self).next(context)
        self.log(self.function_log(value))
        return value, ctx


class FLogContext(FLog):
    def __init__(self, log, level, flux):
        super().__init__(log, level, flux)
        self.function_log = log

    def next(self, context):
        value, ctx = super(FLogContext, self).next(context)
        ctx_bkp = ctx.copy()
        self.log(self.function_log(ctx_bkp))
        return value, ctx


class FMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = super(FMap, self).next(context)
        ctx_bkp = ctx.copy()
        return self.function(value, ctx_bkp), ctx


class FMapContext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = super(FMapContext, self).next(context)
        ctx_bkp = ctx.copy()
        return value, merge(ctx, self.function(value, ctx_bkp))


class FFlatMap(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func
        self.current = None

    def next(self, context):
        ctx = context
        while True:
            while self.current is None:
                value, ctx = super(FFlatMap, self).next(ctx)
                ctx_bkp = ctx.copy()
                self.current = self.function(value, ctx_bkp).subscribe(ctx)
            try:
                v, c = next(self.current)
                while v is None:
                    v, c = next(self.current)
                ctx = merge(ctx, c)
                return v, ctx
            except StopIteration:
                self.current = None


class FDoOnNext(Stream):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def next(self, context):
        value, ctx = super(FDoOnNext, self).next(context)
        self.function(value, ctx)
        return value, ctx
