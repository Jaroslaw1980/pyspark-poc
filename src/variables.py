import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
PATH_TO_DATA_FILES = os.path.join(ROOT, r'raw_data')

file_one = 'dataset_one.csv'
file_two = 'dataset_two.csv'

path_file_one = os.path.join(PATH_TO_DATA_FILES, file_one)
path_file_two = os.path.join(PATH_TO_DATA_FILES, file_two)

OUTPUT_PATH = os.path.join(ROOT, r'client_data')
LOGS_PATH = os.path.join(ROOT, r'logs\codac.log')

LOGGER_NAME = 'Codac logger'
SPARK_APP_NAME = 'CodacApp'
