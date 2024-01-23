from src.file_loader import load_data_from_file


def test_load_data(spark_session, mock_logger, schema):
    load_data_folder = r'C:\Projects\codac\tests\load_test_data'
    schema = schema
    file_format = 'csv'
    df = load_data_from_file(spark_session, file_format, load_data_folder, mock_logger, schema)
    assert df.count() == 10
    assert mock_logger.info.call_count == 4
    assert mock_logger.info.call_args[0][0] == 'Dataframe loaded'
