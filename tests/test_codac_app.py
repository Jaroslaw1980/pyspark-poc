import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.codac_app import CodacApp
from src.utils import create_logger


@pytest.fixture()
def codac(spark_session):
    logger = create_logger()
    return CodacApp(spark_session, logger)


@pytest.fixture()
def schema():
    schema = StructType([
        StructField('id', IntegerType(), nullable=False),
        StructField('first_name', StringType(), nullable=False),
        StructField('last_name', StringType(), nullable=False),
    ])
    return schema


@pytest.fixture()
def schema2():
    schema = StructType([
        StructField('id', IntegerType(), nullable=False),
        StructField('city', StringType(), nullable=False),
        StructField('address', IntegerType(), nullable=False),
    ])
    return schema


@pytest.fixture()
def schema3():
    schema = StructType([
        StructField('id', IntegerType(), nullable=False),
        StructField('first_name', StringType(), nullable=False),
        StructField('last_name', StringType(), nullable=False),
        StructField('city', StringType(), nullable=False),
        StructField('address', IntegerType(), nullable=False),
    ])
    return schema


@pytest.fixture()
def data():
    data = [
        (1, 'Hosea', 'Odonnell'),
        (2, 'Murray', 'Weber'),
        (3, 'Emory', 'Giles'),
        (4, 'Devin', 'Ayala'),
        (5, 'Rebekah', 'Rosario'),
        (6, 'Tracy', 'Gardner'),
        (7, 'Hosea', 'Blackwell'),
        (8, 'Madeline', 'Black'),
        (9, 'Jim', 'Delacruz'),
        (10, 'Abigail', 'Giles')
    ]
    return data


@pytest.fixture()
def data2():
    data = [
        (1, 'London', 233),
        (2, 'Berlin', 43),
        (3, 'Paris', 34),
        (4, 'Warszawa', 5),
        (5, 'Prague', 2),
        (6, 'Rome', 99),
        (7, 'Dublin', 65),
        (8, 'Mardid', 56),
        (9, 'Athens', 25),
        (10, 'Stockholm', 29)
    ]
    return data


@pytest.fixture()
def data3():
    data = [
        (1, 'Hosea', 'Odonnell', 'London', 233),
        (2, 'Murray', 'Weber', 'Berlin', 43),
        (3, 'Emory', 'Giles', 'Paris', 34),
        (4, 'Devin', 'Ayala', 'Warszawa', 5),
        (5, 'Rebekah', 'Rosario', 'Prague', 2),
        (6, 'Tracy', 'Gardner', 'Rome', 99),
        (7, 'Hosea', 'Blackwell', 'Dublin', 65),
        (8, 'Madeline', 'Black', 'Mardid', 56),
        (9, 'Jim', 'Delacruz', 'Athens', 25),
        (10, 'Abigail', 'Giles', 'Stockholm', 29)
    ]
    return data


@pytest.fixture
def df_for_tests(spark_session, data, schema):
    return spark_session.createDataFrame(data=data, schema=schema)


@pytest.fixture()
def df_for_tests2(spark_session, data2, schema2):
    return spark_session.createDataFrame(data=data2, schema=schema2)


@pytest.fixture()
def df_joined(spark_session, data3, schema3):
    return spark_session.createDataFrame(data=data3, schema=schema3)


def test_filter_data(spark_session, schema, df_for_tests, codac):
    remaining_data = [
        (1, 'Hosea', 'Odonnell'),
        (5, 'Rebekah', 'Rosario'),
        (7, 'Hosea', 'Blackwell')
    ]
    remaning_df = spark_session.createDataFrame(data=remaining_data, schema=schema)
    filters = ['Hosea', 'Rebekah']
    filtered = codac.filter_data(df_for_tests, 'first_name', filters)
    assert_df_equality(remaning_df, filtered)


def test_drop_column(df_for_tests, codac):
    num_columns = len(df_for_tests.columns)
    dropped_df = codac.drop_column(df_for_tests, ['id'])
    assert num_columns != len(dropped_df.columns)


def test_rename_column(df_for_tests, codac):
    column_name_mapping = {'id': 'client_identifier'}
    column_name = 'client_identifier'
    df_renamed = codac.rename_column(df_for_tests, column_name_mapping)
    assert column_name in df_renamed.columns


def test_join_dfs(df_for_tests, df_for_tests2, df_joined, codac):
    df_result = codac.join_dfs(df_for_tests, df_for_tests2, on='id')
    assert_df_equality(df_result, df_joined)
