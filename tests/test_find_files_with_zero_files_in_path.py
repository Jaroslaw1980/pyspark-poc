import pytest

from src.file_loader import find_files_in_path


def test_find_files_with_zero_files(tmpdir, mock_logger):

    path = str(tmpdir)

    with pytest.raises(FileNotFoundError):
        find_files_in_path(path, mock_logger)

    assert mock_logger.error.call_count == 1
    assert mock_logger.error.call_args[0][0] == 'Found zero files'
    assert mock_logger.info.call_count == 0
