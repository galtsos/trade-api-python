import pickle
from typing import Dict, Sequence, Set, Tuple


class Singleton(type):
    """
    Requirement to return an old object are determined by calculating hash of a constructor call
    arguments. One issue with this logic is that if the constructor in second time was called
    with keyword-style of some arguments, the logic will fail if it was called with only
    position-style of the same arguments. In this situation a new object will be created.
    """

    _instances = {}
    _hash_args = []
    _hash_kwargs = {}

    def __new__(mcs, *args, singleton_hash_args=None, singleton_hash_kwargs=None, **kwargs):
        mcs._hash_args = singleton_hash_args if singleton_hash_args is not None else []
        mcs._hash_kwargs = singleton_hash_kwargs if singleton_hash_kwargs is not None else {}

        return super().__new__(mcs, *args, **kwargs)

    def __call__(cls, *args, **kwargs):
        args_hash = cls._calculate_args_hash(args, kwargs)
        key = (cls, args_hash)

        if key not in cls._instances:
            cls._instances[key] = super().__call__(*args, **kwargs)

        return cls._instances[key]

    @classmethod
    def _calculate_args_hash(mcs, args: Tuple, kwargs: Dict) -> int:
        filtered_args = [args[i] for i in range(len(args)) if i in mcs._hash_args]
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in mcs._hash_kwargs}
        hash_source = (filtered_args, filtered_kwargs)

        result = hash(pickle.dumps(hash_source, protocol=pickle.HIGHEST_PROTOCOL))

        return result


def find_duplicates_in_list(source_list: Sequence) -> Set:
    return set([tag for tag in source_list if source_list.count(tag) > 1])
