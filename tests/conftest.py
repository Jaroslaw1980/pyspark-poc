import pytest
from pyspark.sql import SparkSession


@pytest.fixture(name='spark_session', scope='session')
def spark_session(request):
    spark = SparkSession.builder \
        .appName('codac_tests') \
        .master('local') \
        .getOrCreate()
    yield spark
    spark.stop()
