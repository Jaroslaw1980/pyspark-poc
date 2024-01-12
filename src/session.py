from pyspark.sql import SparkSession

from src.variables import SPARK_APP_NAME


def start_session():
    """Method used to initiate spark session"""
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master('local') \
        .getOrCreate()
    return spark
