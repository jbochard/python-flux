from python_flux.flux import Flux


def from_generator(generator):
    return PFromGenerator(generator)


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
            self.iterator = iterator if type(iterator) is iter else iter(iterator)
        except TypeError as e:
            raise e

    def next(self, context):
        value = next(self.iterator)
        while value is None:
            value = next(self.iterator)
        return value, context


class PFromGenerator(Producer):
    def __init__(self, generator):
        super(PFromGenerator, self).__init__()
        if type(generator) == Flux:
            self.generator = generator
        else:
            raise Exception("generator must be a Flux")

    def next(self, context):
        value = next(self.generator)
        while value is None:
            value = next(self.generator)
        return value, context
