import pytest

# content of test_sample1.py


def func(x):
    return x + 1


def test_answer1():
    assert func(4) == 5


@pytest.mark.skip()
def test_answer2():
    assert func(4) == 5
