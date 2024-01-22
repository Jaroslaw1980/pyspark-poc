from src.file_loader import find_files_in_path
from src.variables import file_one, file_two
import pytest
import src


def test_find_files_with_valid_path(mock_logger, tmpdir):
    # test for finding files in path folder
    tmpdir.join("data_file_one.csv").write("")
    tmpdir.join("data_file_two.csv").write("")

    path = str(tmpdir)
    find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 0
    assert mock_logger.info.call_count == 1
    assert mock_logger.info.call_args[0][0] == 'Found 2 files, returning files names'
    assert src.variables.file_one == 'data_file_one.csv'
    assert src.variables.file_two == 'data_file_two.csv'


def test_find_files_with_zero_files(tmpdir, mock_logger):
    # temporary directory without files
    path = str(tmpdir)

    with pytest.raises(FileNotFoundError):
        find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 1
    assert mock_logger.error.call_args[0][0] == 'Found zero files'
    assert mock_logger.info.call_count == 0


def test_find_files_with_wrong_number_of_files(tmpdir, mock_logger):
    # temporary directory with more than 2 files
    tmpdir.join("file1.csv").write("")
    tmpdir.join("file2.csv").write("")
    tmpdir.join("file3.csv").write("")

    path = str(tmpdir)

    with pytest.raises(Exception):
        find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 1
    assert mock_logger.error.call_args[0][0] == 'There are wrong number of files in the folder'
    assert mock_logger.info.call_count == 0
