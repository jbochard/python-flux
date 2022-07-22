import glob
import os
import time
import traceback


class Flux(object):
    def __init__(self, flux):
        self.prev = flux

    def _next(self):
        for value in self.prev._next():
            yield value

    def filter(self, f):
        return FFilter(f, self)

    def map(self, f):
        return FMap(f, self)

    def flat_map(self, f):
        return FFlatMap(f, self)

    def delay(self, d):
        return FDelay(d, self)

    def take(self, n):
        return FTake(n, self)

    def cache_to_files(self, key_func, serialize_func, deserialize_func, directory="./cache", prefix="cache", enabled=True):
        return FCacheToFiles(key_func, serialize_func, deserialize_func, directory, prefix, enabled, self)

    def log(self, log=lambda v: str(v)):
        return FLog(log, self)

    def subscribe(self, on_success=lambda v: print(str(v)), on_error=lambda e: print(f"Exception: {e}")):
        FSubscribe(on_success, on_error, self).run()


class FFilter(Flux):
    def __init__(self, p, flux):
        super().__init__(flux)
        self.predicate = p

    def _next(self):
        for value in super(FFilter, self)._next():
            if value is not None:
                if self.predicate(value):
                    yield value


class FTake(Flux):
    def __init__(self, count, flux):
        super().__init__(flux)
        self.count = count
        self.idx = 0

    def _next(self):
        for value in super(FTake, self)._next():
            if value is not None:
                self.idx = self.idx + 1
                if self.idx <= self.count:
                    yield value
                else:
                    break


class FDelay(Flux):
    def __init__(self, delay, flux):
        super().__init__(flux)
        self.delay = delay

    def _next(self):
        for value in super(FDelay, self)._next():
            if value is not None:
                yield value
                time.sleep(self.delay)


class FCacheToFiles(Flux):
    def __init__(self, key_func, serialize_func, deserialize_func, directory, prefix, enabled, flux):
        super().__init__(flux)
        self.key_func = key_func
        self.serialize_func = serialize_func
        self.deserialize_func = deserialize_func
        self.directory = directory
        self.prefix = prefix
        self.enabled = enabled

    def _next(self):
        if self.enabled:
            exists = os.path.exists(self.directory)
            if not exists:
                os.mkdir(self.directory)

            is_dir = os.path.isdir(self.directory)
            if not is_dir:
                raise Exception(f"No se pudo crear directorio {self.directory}. Existe un archivo con el mismo nombre")

            files = glob.glob(f"{self.directory}/{self.prefix}_*.txt")
            if len(files) == 0:
                for value in super(FCacheToFiles, self)._next():
                    if value is not None:
                        key = self.key_func(value)
                        line = self.serialize_func(value)
                        with open(f"{self.directory}/{self.prefix}_{key}.txt", "w") as f:
                            f.write(line)
                        yield value
            else:
                for file in files:
                    with open(file, "r") as f:
                        line = f.readline()
                        yield self.deserialize_func(line)
        else:
            for value in super(FCacheToFiles, self)._next():
                yield value


class FLog(Flux):
    def __init__(self, log, flux):
        super().__init__(flux)
        self.function_log = log

    def _next(self):
        for value in super(FLog, self)._next():
            if value is not None:
                print(f"{str(self.function_log(value))}", flush=True)
                yield value


class FMap(Flux):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def _next(self):
        for value in super(FMap, self)._next():
            if value is not None:
                yield self.function(value)


class FFlatMap(Flux):
    def __init__(self, func, flux):
        super().__init__(flux)
        self.function = func

    def _next(self):
        for value in super(FFlatMap, self)._next():
            if value is not None:
                for v in self.function(value):
                    if v is not None:
                        yield v


class FSubscribe(object):
    def __init__(self, on_success, on_error, f):
        self.on_success = on_success
        self.on_error = on_error
        self.flux = f

    def run(self):
        try:
            for value in self.flux._next():
                if value is not None:
                    self.on_success(value)
        except Exception as e:
            self.on_error(e)
            traceback.print_exc()

