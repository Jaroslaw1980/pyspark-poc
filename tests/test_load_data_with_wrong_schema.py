from src.file_loader import load_data_from_file
from src.schemas import Schemas


def test_load_data_with_wrong_schema(spark_session, logger, caplog):
    caplog.set_level("INFO")
    schemas = [Schemas.dataset_one_schema, Schemas.dataset_two_schema]
    load_data_path = r'C:\Projects\codac\tests\load_test_data\names.csv'
    df = load_data_from_file(spark_session, load_data_path, logger, schemas)

    assert 'There is no correct schema for dataframe' in caplog.text
    assert 'There is no correct schema for dataframe' in caplog.text
    assert df is not None
