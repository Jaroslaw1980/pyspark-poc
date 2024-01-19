def test_drop_column(df_for_tests, codac):
    num_columns = len(df_for_tests.columns)
    dropped_df = codac.drop_column(df_for_tests, ['id'])
    assert num_columns != len(dropped_df.columns)
