import unittest
import json
from masschange.utils.misc import flatten_nested_dict, iterate_nested_dict

class UtilsMiscTestCase(unittest.TestCase):

    # valid input
    good_dict = {
        'a': 'A',
        '_': {
            'a_': 'A__',
            'b_': 'B__',
            '_': {
                'a__': 'A__',
                'b__': 'B__'
            }
        },
        'b': 'B',
    }

    # input with collision
    bad_dict = {
        'a': 'A',
        '_': {
            'a': 'A-overwrite'
        }
    }

    def test_iterate_nested_dict(self):
        result = ([(k, v) for k, v in iterate_nested_dict(self.good_dict)])
        expected = [('a', 'A'), ('a_', 'A__'), ('b_', 'B__'), ('a__', 'A__'), ('b__', 'B__'), ('b', 'B')]
        self.assertEqual(expected, result)

    def test_flatten_nested_dict(self):
        result = json.dumps(flatten_nested_dict(self.good_dict), sort_keys = True)
        expected = '{"a": "A", "a_": "A__", "a__": "A__", "b": "B", "b_": "B__", "b__": "B__"}'
        self.assertEqual(expected, result)

        result = json.dumps(flatten_nested_dict(self.bad_dict, ignore_key_collisions=True), sort_keys=True)
        expected = '{"a": "A-overwrite"}'
        self.assertEqual(expected, result)

        # check that throws
        with self.assertRaises(ValueError):
            flatten_nested_dict(self.bad_dict)

        # check the error message
        try:
            flatten_nested_dict(self.bad_dict)
        except ValueError as err:
            expected = 'Encountered duplicate key "a" when attempting to flatten nested dict'
            self.assertEqual(expected, str(err))



