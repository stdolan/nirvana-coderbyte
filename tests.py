import logging
import unittest

from collections import defaultdict
from unittest.mock import Mock, patch

from requests.exceptions import ConnectTimeout

from coalesce_member_data import coalesce_member_data


class TestCoalesceMemberData(unittest.TestCase):
    default_responses = {
        "https://api1.com?member_id=1": {"deductible": 1000, "stop_loss": 10000, "oop_max": 5000},
        "https://api2.com?member_id=1": {"deductible": 1200, "stop_loss": 13000, "oop_max": 6000},
        "https://api3.com?member_id=1": {"deductible": 1000, "stop_loss": 10000, "oop_max": 6000},
        # Including two values that we shouldn't hit.
        "https://api4.com?member_id=1": {"deductible": 0, "stop_loss": 0, "oop_max": 0},
        "https://api3.com?member_id=2": {
            "deductible": 1 << 31,
            "stop_loss": 1 << 31,
            "oop_max": 1 << 31,
        },
    }

    default_return_value = {"deductible": 1066, "stop_loss": 11000, "oop_max": 5666}

    def test_no_member_id(self):
        with self.assertRaises(ValueError):
            coalesce_member_data(None)

    @patch("coalesce_member_data.get")
    def test_connection_timeout(self, get_mock):
        def side_effect(url, timeout):
            if url == "https://api2.com?member_id=1":
                raise ConnectTimeout
            else:
                mock = Mock()
                mock.json.return_value = self.default_responses[url]
                return mock

        get_mock.side_effect = side_effect

        expected = {"deductible": 1000, "stop_loss": 10000, "oop_max": 5500}
        actual = coalesce_member_data(1)

        self.assertEqual(expected, actual)

    @patch("coalesce_member_data.get")
    def test_valid_default_strategy(self, get_mock):
        def side_effect(url, timeout):
            mock = Mock()
            mock.json.return_value = self.default_responses[url]
            return mock

        get_mock.side_effect = side_effect

        expected = {"deductible": 1066, "stop_loss": 11000, "oop_max": 5666}
        actual = coalesce_member_data(1)
        self.assertEqual(expected, actual)

    @patch("coalesce_member_data.get")
    def test_valid_alternate_strategy(self, get_mock):
        def side_effect(url, timeout):
            mock = Mock()
            mock.json.return_value = self.default_responses[url]
            return mock

        get_mock.side_effect = side_effect

        def total(member_data_source):
            field_tracker = defaultdict(int)

            for member_data in member_data_source:
                for field in member_data:
                    field_tracker[field] += member_data[field]

            return field_tracker

        expected = {"deductible": 3200, "stop_loss": 33000, "oop_max": 17000}
        actual = coalesce_member_data(1, total)

        self.assertEqual(expected, actual)


if __name__ == "__main__":
    # We could try to test error messaging, but that's far too brittle IMO.
    logging.disable()
    unittest.main()