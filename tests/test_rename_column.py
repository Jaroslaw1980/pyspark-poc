def test_rename_column(df_for_tests, codac):
    column_name_mapping = {'id': 'client_identifier'}
    column_name = 'client_identifier'
    df_renamed = codac.rename_column(df_for_tests, column_name_mapping)
    assert column_name in df_renamed.columns
