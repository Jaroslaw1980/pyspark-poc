import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
PATH_TO_DATA_FILES = os.path.join(ROOT, r'raw_data')

users = 'dataset_one.csv'
transactions = 'dataset_two.csv'

path_to_users_data = os.path.join(PATH_TO_DATA_FILES, users)
path_to_transactions_data = os.path.join(PATH_TO_DATA_FILES, transactions)

OUTPUT_PATH = os.path.join(ROOT, r'client_data')
LOGS_PATH = os.path.join(ROOT, r'logs\codac.log')

LOGGER_NAME = 'Codac logger'
SPARK_APP_NAME = 'CodacApp'
