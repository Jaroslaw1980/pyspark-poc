from src.file_loader import find_files_in_path
from src.variables import file_one, file_two
import pytest
import src


def test_find_files_with_valid_path(mock_logger, tmpdir):

    tmpdir.join("data_file_one.csv").write("")
    tmpdir.join("data_file_two.csv").write("")

    path = str(tmpdir)
    find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 0
    assert mock_logger.info.call_count == 1
    assert mock_logger.info.call_args[0][0] == 'Found 2 files, setting files names into variables'
    assert src.variables.file_one == 'data_file_one.csv'
    assert src.variables.file_two == 'data_file_two.csv'
