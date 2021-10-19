from ddb_local import LocalDynamoDB, create_new_inmemory_ddb
import requests
import pytest
import boto3


def create_test_table(dynamodb_resource):
    table = dynamodb_resource.create_table(
        TableName="TestTable",
        KeySchema=[
            {"AttributeName": "hash", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "range", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "hash", "AttributeType": "S"},
            {"AttributeName": "range", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    return table


def does_test_table_exists(ddb_client):
    try:
        ddb_client.describe_table(TableName="TestTable")
        return True
    except ddb_client.exceptions.ResourceNotFoundException:
        return False


def test_in_memory():
    db = create_new_inmemory_ddb()
    with db:
        ddb_resource = boto3.resource("dynamodb", endpoint_url=db.endpoint)
        create_test_table(ddb_resource)
        # should be able to create multiple, and should be empty
        with create_new_inmemory_ddb() as db2:
            ddb_client2 = boto3.client("dynamodb", endpoint_url=db2.endpoint)
            assert not does_test_table_exists(ddb_client2)


def test_value_persisted_over_multiple_invocations(default_test_dir):
    with LocalDynamoDB(unpack_dir=default_test_dir) as ddb:
        ddb_resource = boto3.resource("dynamodb", endpoint_url=ddb.endpoint)
        create_test_table(ddb_resource)
    import time

    # I don't think DynamoDBLocal uses reuseaddr, so we have to add an artificial delay
    time.sleep(1)
    with LocalDynamoDB(unpack_dir=default_test_dir) as ddb:
        ddb_client = boto3.client("dynamodb", endpoint_url=ddb.endpoint)
        assert does_test_table_exists(ddb_client)


def test_port_check(default_test_dir):
    with LocalDynamoDB(unpack_dir=default_test_dir) as ddb:
        with pytest.raises(Exception):
            with LocalDynamoDB(unpack_dir=default_test_dir) as ddb2:
                pass
        pass


def test_dont_accept_both_in_memory_and_db_path(default_test_dir):
    with pytest.raises(Exception):
        LocalDynamoDB(in_memory=True, db_path=".")


def test_with_java_home(java_home, default_test_dir):
    with LocalDynamoDB(unpack_dir=default_test_dir) as ddb:
        requests.get("http://localhost:8000")
        pass


def test_fail_to_start(default_test_dir):
    with pytest.raises(Exception):
        with LocalDynamoDB(unpack_dir=default_test_dir, extra_args=["-TRASH"]):
            pass


def test_clean_dir_start(clean_dir):
    with LocalDynamoDB(unpack_dir=clean_dir):
        pass


def test_unusable_unpack_dir(existing_file):
    with pytest.raises(Exception):
        with LocalDynamoDB(unpack_dir=existing_file):
            pass
