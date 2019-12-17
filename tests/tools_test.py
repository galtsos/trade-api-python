import pytest

from galts_trade_api.tools import Singleton


def fixture_foo():
    class Foo(metaclass=Singleton):
        pass

    yield Foo(), Foo(), True

    class Bar(
        metaclass=Singleton,
        singleton_hash_args=[0],
        singleton_hash_kwargs=['baz']
    ):
        def __init__(self, foo, bar, baz='str'):
            self._foo = foo
            self._bar = bar
            self._baz = baz

    inst1 = Bar('foo_value1', 'bar_value1', baz='baz_value1')
    inst2 = Bar('foo_value1', 'bar_value2', baz='baz_value1')
    inst3 = Bar('foo_value2', 'bar_value3', baz='baz_value1')
    inst4 = Bar('foo_value1', 'bar_value4', baz='baz_value2')
    inst5 = Bar('foo_value2', 'bar_value5', baz='baz_value2')

    yield inst1, inst2, True
    yield inst1, inst3, False
    yield inst1, inst4, False
    yield inst1, inst5, False


class TestSingleton:
    @staticmethod
    @pytest.mark.parametrize('inst1, inst2, should_be_same', fixture_foo())
    def test_instances_same(inst1, inst2, should_be_same):
        if should_be_same:
            assert inst1 is inst2
        else:
            assert inst1 is not inst2
