import itertools


def chunked_iterable(iterable, batch_size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, batch_size))
        if not chunk:
            break
        yield chunk
