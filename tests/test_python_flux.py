import datetime

from python_flux.producers import from_iterator, from_generator


def test_iterator():
    flux = from_iterator(range(0, 10))
    assert flux.to_list() == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_foreach():
    tmp = []
    from_iterator(range(0, 10))\
        .foreach(on_success=lambda v, c: tmp.append(v))

    assert tmp == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_filter():
    flux = from_iterator(range(0, 10))\
        .filter(lambda v, c: v % 2 == 0)
    assert flux.to_list() == [0, 2, 4, 6, 8]


def test_do_on_next():
    flux = from_iterator(range(0, 10))\
        .do_on_next(lambda v, c: print(f"Valor: {v}"), lambda e, c: print(f"Error: {type(e)} - {str(e)}"))
    flux.foreach()


def test_on_error_resume():
    flux = from_iterator(range(-4, 4))\
        .map(lambda v, c: round(1/v, 1))\
        .on_error_resume(lambda e, c: 1000)
    assert flux.to_list() == [-0.2, -0.3, -0.5, -1.0, 1000, 1.0, 0.5, 0.3]


def test_on_error_retry():
    def add_to_result(v, c):
        result.append(v)
        return round(1/v, 1)

    result = []
    flux = from_iterator(range(-4, 4))\
        .map(add_to_result)\
        .on_error_retry(retries=2)\
        .on_error_resume(lambda e, c: 2000)
    assert flux.to_list() == [-0.2, -0.3, -0.5, -1.0, 2000, 1.0, 0.5, 0.3]
    assert result == [-4, -3, -2, -1, 0, 0, 0, 1, 2, 3]


def test_map():
    flux = from_iterator(range(0, 10))\
        .map(lambda v, c: v + 1)
    assert flux.to_list() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_flatmap():
    flux = from_iterator(range(0, 3))\
        .flat_map(lambda v, c: from_iterator(range(0, v + 1)))
    assert flux.to_list() == [0, 0, 1, 0, 1, 2]


def test_take():
    flux = from_iterator(range(0, 10))\
        .take(3)
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
