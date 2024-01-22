import pytest

from src.file_loader import find_files_in_path


def test_find_files_with_wrong_number_of_files(tmpdir, mock_logger):
    tmpdir.join("file1.csv").write("")
    tmpdir.join("file2.csv").write("")
    tmpdir.join("file3.csv").write("")

    path = str(tmpdir)

    with pytest.raises(Exception):
        find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 1
    assert mock_logger.error.call_args[0][0] == 'There are wrong number of files in the folder'
    assert mock_logger.info.call_count == 0
