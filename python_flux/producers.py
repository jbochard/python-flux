import glob

from python_flux.flux import Flux


def from_callable(callable_gen):
    return PFromCallable(callable_gen)


def from_iterator(iterator):
    return PFromIterator(iterator)


def from_directory(regex):
    return PFromDirectory(regex)


class Producer(Flux):
    def __init__(self):
        super(Producer, self).__init__()

    def _next(self, context):
        return self._gen(context)


class PFromIterator(Producer):
    def __init__(self, iterator):
        super(PFromIterator, self).__init__()
        try:
            self.iterator = iter(iterator)
        except TypeError as e:
            raise e

    def _gen(self, context):
        for value in self.iterator:
            if value is not None:
                yield value, context


class PFromDirectory(Producer):
    def __init__(self, regex):
        super(PFromDirectory, self).__init__()
        self.regex = regex

    def _gen(self, context):
        files = glob.glob(self.regex)
        for file in files:
            with open(file, "r", newline="") as f:
                line = f.readline()
                if line is not None:
                    yield line, context


class PFromCallable(Producer):
    def __init__(self, callable_gen):
        super(PFromCallable, self).__init__()
        self.function_gen = callable_gen

    def _gen(self, context):
        for value in self.function_gen(context):
            if value is not None:
                yield value, context
