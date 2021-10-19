# ddb-local

Python wrapper for DynamoDB Local.

## What is it for?

This is a convenient Python wrapper for [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html). All the instructions online currently involves manually downloading the tarball and installing it, which is a huge hassle and is a barrier to automation. It wasn't really easy to integrate the local DDB nicely into tests (e.g., pytest) so I wrote this library.

## Prerequisite

You must have `java` in `PATH` or specify `JAVA_HOME`. You need a working Internet connection to download DynamoDBLocal tarball. Optionally, you can get it yourself, and point `unpack_dir` to the root of the unpacked directory. 

## Examples

Get a throwaway in-memory DDB for testing:

```python
import boto3
from ddb_local import create_new_inmemory_ddb

with create_new_inmemory_ddb() as local_ddb:
    ddb = boto3.client('dynamodb', endpoint_url=local_ddb.endpoint)
```

Use it in a context manager:

```python
import boto3
from ddb_local import LocalDynamoDB

with LocalDynamoDB() as local_ddb:
    ddb = boto3.client('dynamodb', endpoint_url=local_ddb.endpoint)

```

Example usage with [pytest](https://pytest.org/):

```python
import pytest
from ddb_local import LocalDynamoDB

@pytest
def ddb():
    with LocalDynamoDB():
        yield ddb
```

## Development

* `make` to run test, coverage and distribution build.
* `make coverage` to run coverage.
* `make test` to run test.
* `pytest` to run test.
* `make clean` to delete all files.
* `make upload` to upload to PyPI.
* Populate `.pypirc` with the API token.

## Support

Support is provided on a best-effort basis. 

Create an issue in the [Github repo](https://github.com/wbkang/ddb_local).