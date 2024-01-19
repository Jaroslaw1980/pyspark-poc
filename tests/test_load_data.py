def test_load_data(codac, schema):
    load_data_path = r'C:\Projects\codac\tests\load_test_data\names.csv'
    df = codac.load_data(load_data_path, schema)
    print(df.schema)
    assert df is not None
    df.show()
