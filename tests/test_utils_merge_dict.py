import pytest
from lib.utils.merge_dict import merge_dict

def test_merge_dict():
    dict1 = {1: 'apple', 2: 'ball'}
    dict2 = {'name':'Jack', 'age': 26, 'phone': ['number1', 'number2']}
    output_dict = merge_dict(dict1, dict2)
    expected_dict = {'name':'Jack', 'age': 26, 'phone': ['number1', 'number2'], 1: 'apple', 2: 'ball'}
    assert output_dict == expected_dict
