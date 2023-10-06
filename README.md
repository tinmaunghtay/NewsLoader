## The New York Times Articles Loader
The loader loader.py can load the articles as required but please note that due to default limitations, wide range of queries such as "Silicon Valley" will not able to run fully. Thus, please use another query which has less articles (hits) to test. Otherwise, "hits size is more than 500 requests per day limit".

#### normalize function
- normalize function is only put together with loader for now for simplicity. 
- Each list inside the individual articles are flattened as 0.<attributeX>.<attributeY> to n.<attribute>.<attributeY> or  <attributeX>.0.<attributeY>
- Function performance can be improved further.
#### To run
- use source-files/analysis/loader.py to run
- please note that api key and query should be changed before running the loader.

#### To test
- use tests/test_loader.py
- please note that only a few scenarios are covered.



