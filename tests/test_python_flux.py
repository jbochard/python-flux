import datetime

from python_flux.producers import from_iterator, from_generator


def test_iterator():
    flux = from_iterator(range(0, 10))
    assert flux.to_list() == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_foreach():
    tmp = []
    from_iterator(range(0, 10))\
        .foreach(on_success=lambda v: tmp.append(v))

    assert tmp == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_filter():
    flux = from_iterator(range(0, 10))\
        .filter(lambda v: v % 2 == 0)
    assert flux.to_list() == [0, 2, 4, 6, 8]


def test_map():
    flux = from_iterator(range(0, 10))\
        .map(lambda v, c: v + 1)
    assert flux.to_list() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_flatmap():
    def tmp(v, c):
        return from_iterator((0, 3))\
            .map(lambda i, ic: v + i)

    flux = from_iterator(range(0, 5))\
        .flat_map(tmp)
    assert flux.to_list() == [0, 3, 1, 4, 2, 5, 3, 6, 4, 7]


def test_take():
    flux = from_iterator(range(0, 10))\
        .take(3)
    assert flux.to_list() == [0, 1, 2]


def test_take_during_timedelta():
    flux = from_iterator(range(0, 10))\
        .delay(1)\
        .take_during_timedelta(datetime.timedelta(seconds=4))
    assert flux.to_list() == [0, 1, 2]


def test_map_context():
    def inc(v, c):
        return {'inc': c['inc'] + 1}

    flux = from_iterator(range(0, 10))\
        .map_context(inc) \
        .map(lambda v, c: c['inc'])

    assert flux.to_list(context={"inc": 1, "test": True}) == [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]


def test_from_generator():
    def generator(context):
        for i in range(10):
            yield i

    flux = from_generator(generator)
    assert flux.to_list() == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


if __name__ == "__main__":
    test_iterator()
    test_take()
    test_filter()
    test_map()
    test_map_context()
    test_flatmap()
    test_from_generator()
