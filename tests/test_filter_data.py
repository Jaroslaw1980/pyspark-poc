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
