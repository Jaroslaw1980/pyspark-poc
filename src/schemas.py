from dataclasses import dataclass

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@dataclass
class Schemas:
    dataset_one_schema = StructType([
        StructField('id', IntegerType()),
        StructField('first_name', StringType()),
        StructField('last_name', StringType()),
        StructField('email', StringType()),
        StructField('country', StringType()),
    ])

    dataset_two_schema = StructType([
        StructField('id', IntegerType()),
        StructField('btc_a', StringType()),
        StructField('cc_t', StringType()),
        StructField('cc_n', StringType()),
    ])
