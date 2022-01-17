
import itertools
from typing import Iterable, Tuple


def chunked_iterable(iterable: Iterable, size: int) -> Tuple:
    """Yield successive n-sized chunks from iterable.
    :param iterable: Iterable to chunk
    :param size: Chunks size
    :yield: Chunk in tuple format
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def pprint_dict(dict_: dict):
    for key, value in dict_.items():
        print(f'{key:<30} {value:<30}')
