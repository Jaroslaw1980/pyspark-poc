import os
from pathlib import Path

ROOT = Path(__file__).parent.parent

PATH_FILE_ONE = os.path.join(ROOT, r'raw_data\dataset_one.csv')
PATH_FILE_TWO = os.path.join(ROOT, r'raw_data\dataset_two.csv')
OUTPUT_PATH = os.path.join(ROOT, r'client_data')
LOGS_PATH = os.path.join(ROOT, r'logs\codac.log')

LOGGER_NAME = 'Codac logger'
SPARK_APP_NAME = 'CodacApp'
