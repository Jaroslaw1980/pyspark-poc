from chispa.dataframe_comparer import assert_df_equality


def test_join_dfs(df_for_tests, df_for_tests2, df_joined, codac):
    df_result = codac.join_dfs(df_for_tests, df_for_tests2, on='id')
    assert_df_equality(df_result, df_joined, ignore_row_order=True)