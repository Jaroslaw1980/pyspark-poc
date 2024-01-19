import os
from pathlib import Path


def test_save_data(df_for_tests, codac):
    path_to_folder = r'C:\Projects\codac\tests\save_test_data'
    codac.save_data(df_for_tests, 'csv', path_to_folder)
    expected = len(os.listdir(path_to_folder))
    assert expected == 4

    # clean directory for another test
    directory_path = Path(path_to_folder)
    files_to_delete = directory_path.glob("*.*")
    for file in files_to_delete:
        file.unlink()
