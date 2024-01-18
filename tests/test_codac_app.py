from chispa.dataframe_comparer import assert_df_equality


def test_filter_data(spark_session, schema, df_for_tests, codac):
    remaining_data = [
        (1, 'Hosea', 'Odonnell'),
        (5, 'Rebekah', 'Rosario'),
        (7, 'Hosea', 'Blackwell')
    ]
    remaning_df = spark_session.createDataFrame(data=remaining_data, schema=schema)
    filters = ['Hosea', 'Rebekah']
    filtered = codac.filter_data(df_for_tests, 'first_name', filters)
    assert_df_equality(remaning_df, filtered, ignore_row_order=True)


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
    assert_df_equality(df_result, df_joined, ignore_row_order=True)
