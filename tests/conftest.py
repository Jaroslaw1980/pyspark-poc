import pytest
from pyspark.sql import SparkSession

from src.codac_app import CodacApp
from src.utils import create_logger


@pytest.fixture(name='spark_session', scope='session')
def spark_session(request):
    spark = SparkSession.builder \
        .appName('codac_tests') \
        .master('local') \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(name='codac', scope='session')
def codac(spark_session):
    logger = create_logger()
    return CodacApp(spark_session, logger)
