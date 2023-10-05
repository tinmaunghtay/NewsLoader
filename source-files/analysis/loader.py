import argparse
import logging
import requests as req
import time
from typing import Any

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        """Empty Init for now"""
        # Nothing to do
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        """Connect has nothing to do now"""
        # Nothing to do
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """

        if batch_size != 10:
            raise ValueError("batch_size must be 10")

        req_headers = {
            "Accept": "application/json"
        }

        batch_news = []

        url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?q=' + self.args.query + '&api-key=' + self.args.api_key + '&page=0'
        response = req.get(url, req_headers).json()
        hits = response['response']['meta']['hits']
        batch_news.append(self.list_after_normalise(response['response']['docs']))

        pages = 1
        if hits <= 10:
            pages = 0
        elif hits > 10:
            pages = hits // 10
            if pages > 500:
                raise ValueError("hits size is more than 500 requests per day limit, "
                                 "try to narrow the results by changing the query")

        # 100 pages limit thus setting 100, 10 records per page is a limitation
        for i in range(1, pages):
            url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?q=' + self.args.query + '&api-key=' + self.args.api_key + '&page=' + str(
                i)
            response = req.get(url, req_headers).json()
            docs = response['response']['docs']
            news_list = self.list_after_normalise(docs)
            # sleep for 12s to avoid an issue with 5 requests per minute rate limit
            time.sleep(12)
            batch_news.append(news_list)

        return batch_news

    def list_after_normalise(self, docs):
        """
        A function that lists all documents that have been normalized
        :param docs: list of docs
        :return: list of news from each page (10 records)
        """
        news_list = []

        for doc in docs:
            ny_dict = {}
            ny_dict = self._normalise_json(doc, '', ny_dict, '.')
            news_list.append(ny_dict)

        return news_list

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema

    def _normalise_json(
            self,
            data: Any,
            key_string: str,
            normalized_dict: dict[str, Any],
            separator: str,
    ) -> dict[str, Any]:
        """
        A Function to normalize json data and
        return a dictionary which contains the flattened attributes

        Parameters
        ----------
        data : Any
            Type dependent on types contained within nested Json
        key_string : str
            New key (with separator(s) in) for data
        normalized_dict : dict
            The new normalized/flattened Json dict
        separator : str, default '.'
            Nested records will generate names separated by sep,
            e.g., for sep='.', { 'foo' : { 'bar' : 0 } } -> foo.bar
        """

        if not isinstance(data, dict):
            normalized_dict[key_string] = data
        else:
            for key, value in data.items():
                new_key: str = f"{key_string}{separator}{key}"

                if not key_string:
                    new_key = new_key.removeprefix(separator)

                if isinstance(value, list):
                    for idx, i in enumerate(value):
                        new_key = f"{key_string}{separator}{idx}{separator}{key}"
                        if not key_string:
                            new_key = new_key.removeprefix(separator)
                        self._normalise_json(
                            data=i,
                            key_string=new_key,
                            normalized_dict=normalized_dict,
                            separator=separator,
                        )
                else:
                    self._normalise_json(
                        data=value,
                        key_string=new_key,
                        normalized_dict=normalized_dict,
                        separator=separator,
                    )

        return normalized_dict


if __name__ == "__main__":
    config = {
        "api_key": "gt6SvGFDssY29atL0bjYdorCWElrySTc",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")
