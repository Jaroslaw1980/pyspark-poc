from src.file_loader import load_data_from_file
from src.schemas import Schemas


def test_load_data_with_wrong_schema(spark_session, mock_logger):
    schemas = [Schemas.dataset_one_schema, Schemas.dataset_two_schema]
    load_data_path = r'C:\Projects\codac\tests\load_test_data\names.csv'
    df = load_data_from_file(spark_session, load_data_path, mock_logger, schemas)

    assert mock_logger.error.call_count == 2
    assert mock_logger.error.call_args[0][0] == 'There is no correct schema for dataframe'
    assert df is not None
