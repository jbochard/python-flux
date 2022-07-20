import glob

from flux.flux import Flux


def from_iterator(iterator):
    return FFromIterator(iterator)


def from_directory(regex):
    return FFromDirectory(regex)


class Producer(Flux):
    def __init__(self):
        super(Producer, self).__init__(None)

    def _next(self):
        return self._gen()


class FFromIterator(Producer):
    def __init__(self, iterator):
        super(FFromIterator, self).__init__()
        if type(iterator) is not iter:
            self.iterator = iter(iterator)
        else:
            self.iterator = iterator

    def _gen(self):
        for value in self.iterator:
            yield value


class FFromDirectory(Producer):
    def __init__(self, regex):
        self.regex = regex

    def _gen(self):
        files = glob.glob(self.regex)
        for file in files:
            line = None
            with open(file, "r", newline="") as f:
                line = f.readline()
            if line is not None:
                yield line

