def test_load_data_without_file(codac, schema):
    empty_data_folder = r'C:\Projects\codac\tests\e'
    codac.load_data(empty_data_folder, schema=schema)
    assert f'File in path: {empty_data_folder} do not exists'
