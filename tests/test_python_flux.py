from python_flux.producers import from_iterator, from_callable


def test_iterator():
    flux = from_iterator(range(0, 10))
    assert list(flux.subscribe()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_filter():
    flux = from_iterator(range(0, 10))\
        .filter(lambda v: v % 2 == 0)
    assert list(flux.subscribe()) == [0, 2, 4, 6, 8]


def test_map():
    flux = from_iterator(range(0, 10))\
        .map(lambda v, c: v + 1)
    assert list(flux.subscribe()) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_flatmap():
    def tmp(v, c):
        for i in range(0, 3):
            yield v + i

    flux = from_iterator(range(0, 5))\
        .flat_map(tmp)
    assert list(flux.subscribe()) == [0, 1, 2, 1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6]


def test_take():
    flux = from_iterator(range(0, 10))\
        .take(3)
    assert list(flux.subscribe()) == [0, 1, 2]


def test_map_context():
    flux = from_iterator(range(0, 10))\
        .map_context(lambda v, c: {"inc": c["inc"] + 1})\
        .map(lambda v, c: v + c['inc'])
    assert list(flux.subscribe(context={"inc": 0})) == [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]


def test_from_callable():
    def context_func():
        return {'count': 15}

    def iterable(ctx):
        for i in range(0, ctx['count']):
            yield i

    flux = from_callable(iterable)
    assert list(flux.subscribe(context=context_func)) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]


if __name__ == "__main__":
    test_iterator()
    test_take()
    test_filter()
    test_map()
    test_map_context()
    test_flatmap()
    test_from_callable()
