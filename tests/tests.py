import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


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
        StructField('f_name', StringType(), nullable=False),
        StructField('l_name', StringType(), nullable=False),
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


@pytest.fixture
def df_for_tests(spark_session, data, schema):
    return spark_session.createDataFrame(data=data, schema=schema)


@pytest.fixture()
def df_for_tests2(spark_session, data, schema2):
    return spark_session.createDataFrame(data=data, schema=schema2)


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


def test_join_dfs(df_for_tests, df_for_tests2, codac):
    joined_dtf = codac.join_dfs(df_for_tests, df_for_tests2, on='id')
    assert len(joined_dtf.columns) == 5
