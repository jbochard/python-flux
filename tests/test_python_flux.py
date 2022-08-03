from python_flux.producers import from_iterator, from_generator


def test_iterator():
    flux = from_iterator(range(0, 10))
    assert list(flux.subscribe()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_foreach():
    tmp = []
    from_iterator(range(0, 10))\
        .foreach(on_success=lambda v: tmp.append(v))

    assert tmp == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


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
        return from_iterator((0, 3))\
            .map(lambda i, ic: v + i)

    flux = from_iterator(range(0, 5))\
        .flat_map(tmp)
    assert list(flux.subscribe()) == [0, 3, 1, 4, 2, 5, 3, 6, 4, 7]


def test_take():
    flux = from_iterator(range(0, 10))\
        .take(3)
    assert list(flux.subscribe()) == [0, 1, 2]


def test_map_context():
    def add(v, c):
        return v + c['inc']

    def inc(v, c):
        return {'inc': c['inc'] + 1}

    flux = from_iterator(range(0, 10))\
        .map_context(inc) \
        .map(lambda v, c: c['inc'])\
        .subscribe(context={"inc": 1, "test": True})

    assert list(flux) == [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]


def test_from_generator():
    def generator(context):
        for i in range(10):
            yield i

    flux = from_generator(generator)
    assert list(flux.subscribe()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


if __name__ == "__main__":
    test_iterator()
    test_take()
    test_filter()
    test_map()
    test_map_context()
    test_flatmap()
    test_from_generator()
