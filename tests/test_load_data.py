from src.file_loader import load_data_from_file


def test_load_data(spark_session, mock_logger, schema, schema2):
    load_data_folder = r'C:\Projects\codac\tests\load_test_data'
    schemas = [schema, schema2]
    df = load_data_from_file(spark_session, load_data_folder, mock_logger, schemas=schemas)
    assert df.count() == 10
    assert mock_logger.info.call_count == 4
    assert mock_logger.info.call_args[0][0] == 'Dataframe loaded'
