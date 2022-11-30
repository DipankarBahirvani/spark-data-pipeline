import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    yield SparkSession.builder.master("local").appName("run-unit-test").getOrCreate()
