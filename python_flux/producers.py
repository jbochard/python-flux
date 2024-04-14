from python_flux.flux import Flux


def from_generator(generator):
    return PFromGenerator(generator)


def from_iterator(iterator):
    return PFromIterator(iterator)


class Producer(Flux):
    def __init__(self):
        super(Producer, self).__init__()

    def next(self, context):
        return None, None, context


class PFromIterator(Producer):
    def __init__(self, iterator):
        super(PFromIterator, self).__init__()
        self.value = None
        try:
            self.iterator = iterator if type(iterator) is iter else iter(iterator)
        except TypeError as e:
            raise e

    def prepare_next(self):
        self.value = None

    def next(self, context):
        try:
            if self.value is None:
                self.value = next(self.iterator)
            return self.value, None, context
        except Exception as ex:
            return self.value, ex, context


class PFromGenerator(Producer):
    def __init__(self, function_gen):
        super(PFromGenerator, self).__init__()
        self.function_gen = function_gen
        self.generator = None
        self.value = None

    def prepare_next(self):
        self.value = None

    def next(self, context):
        if self.generator is None:
            self.generator = self.function_gen(context)
        try:
            if self.value is None:
                self.value = next(self.generator)
            return self.value, None, context
        except Exception as ex:
            return self.value, ex, context
