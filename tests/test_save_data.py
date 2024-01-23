import os


def test_save_data(df_for_tests, codac, tmpdir):
    path = str(tmpdir)

    codac.save_data(df_for_tests, 'csv', 'overwrite', path)
    expected = len(os.listdir(path))
    assert expected == 4
