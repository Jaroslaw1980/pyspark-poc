def test_load_data_with_wrong_schema(codac, schema2):
    load_data_folder = r'C:\Projects\codac\tests\load_test_data'
    codac.load_data(load_data_folder, schema=schema2)
    assert f'{schema2} for {load_data_folder} is incorrect'
