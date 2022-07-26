import glob

from python_flux.flux import Flux


def from_callable(callable_gen):
    return PFromCallable(callable_gen)


def from_iterator(iterator):
    return PFromIterator(iterator)


class Producer(Flux):
    def __init__(self):
        super(Producer, self).__init__()

    def next(self, context):
        return None


class PFromIterator(Producer):
    def __init__(self, iterator):
        super(PFromIterator, self).__init__()
        try:
            self.iterator = iter(iterator)
        except TypeError as e:
            raise e

    def next(self, context):
        value = next(self.iterator)
        while value is None:
            value = next(self.iterator)
        return value, context


class PFromCallable(Producer):
    def __init__(self, callable_gen):
        super(PFromCallable, self).__init__()
        self.function_gen = callable_gen
        self.parent = None

    def next(self, context):
        if self.parent is None:
            self.parent = self.function_gen(context).subscribe(context)

        value = next(self.parent)
        while value is None:
            value = next(self.parent)
        return value, context


        return next(self.current), context

