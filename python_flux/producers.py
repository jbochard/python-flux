from python_flux.flux import Flux


def from_generator(generator):
    return PFromGenerator(generator)


def from_iterator(iterator):
    return PFromIterator(iterator)


class Producer(Flux):
    def __init__(self):
        super(Producer, self).__init__()
        self.value = None

    def prepare_next(self):
        self.value = None

    def get_value(self):
        return self.value

    def next(self, context):
        if self.value is not None:
            return self.value, None, context
        v, e = self._next(context)
        if e is None:
            self.value = v
            return self.value, None, context
        return None, e, context

    def _next(self, context):
        return None, None


class PFromIterator(Producer):
    def __init__(self, iterator):
        super(PFromIterator, self).__init__()
        try:
            self.iterator = iterator if type(iterator) is iter else iter(iterator)
        except TypeError as e:
            raise e

    def _next(self, context):
        try:
            v = next(self.iterator)
            return v, None
        except Exception as ex:
            return None, ex


class PFromGenerator(Producer):
    def __init__(self, function_gen):
        super(PFromGenerator, self).__init__()
        self.function_gen = function_gen
        self.generator = None

    def _next(self, context):
        if self.generator is None:
            self.generator = self.function_gen(context)
        try:
            v = next(self.generator)
            return v, None
        except Exception as ex:
            return None, ex
