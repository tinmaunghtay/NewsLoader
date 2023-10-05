from unittest import TestCase, main
from analysis.loader import NYTimesSource
import argparse


class TestNYTimesSource(TestCase):

    def test_get_data_batch(self):
        source = NYTimesSource()
        config = {
            "api_key": "gt6SvGFDssY29atL0bjYdorCWElrySTc",
            "query": "Silicon Valley Japan Singapore India",
        }
        source.args = argparse.Namespace(**config)
        print("Testing Results")
        self.assertEqual(len(source.getDataBatch(10)), 6)

        print("Testing ValueError must be 10")
        with self.assertRaises(ValueError) as context:
            source.getDataBatch(100)
        self.assertEqual('batch_size must be 10', str(context.exception))

        print("Testing ValueError must have less than 500 pages")
        config = {
            "api_key": "gt6SvGFDssY29atL0bjYdorCWElrySTc",
            "query": "Silicon Valley",
        }
        source.args = argparse.Namespace(**config)
        with self.assertRaises(ValueError) as context:
            source.getDataBatch(10)
        self.assertEqual('hits size is more than 500 requests per day limit, '
                         'try to narrow the results by changing the query', str(context.exception))


if __name__ == "__main__":
    main()
