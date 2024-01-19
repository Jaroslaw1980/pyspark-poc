import argparse
import logging
import os.path
from logging.handlers import RotatingFileHandler
from pyspark.sql import DataFrame
from src.variables import PATH_FILE_ONE, PATH_FILE_TWO, LOGGER_NAME, LOGS_PATH, ROOT


def create_logger() -> logging.Logger:
    """Initialize rotating logger
    :return Logger
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    log_handler = logging.handlers.RotatingFileHandler(
        filename=LOGS_PATH,
        mode='a',
        maxBytes=10 ** 3 * 3,
        backupCount=5
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%d-%m-%Y %H:%M:%S')

    log_handler.setFormatter(formatter)
    log_handler.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def parse_arguments() -> argparse.Namespace:
    """Function for parsing arguments provided by command line and returning them
    :return Argparse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_one', type=str, help='Path to file one',
                        default=PATH_FILE_ONE)
    parser.add_argument('--file_two', type=str, help='Path to file two',
                        default=PATH_FILE_TWO)
    parser.add_argument('--countries', type=str, nargs='+', help='Countries to filter',
                        default=['Netherlands', 'United Kingdom'])

    args = parser.parse_args()
    return args


def check_if_file_exists(path_to_file: str) -> bool:
    """This function checks if there are files under the path to load
    :param path_to_file: path to a dataset file_one
    :type path_to_file str
    :return True of False depending on the files exists
    :rtype Bool
    """
    return os.path.exists(path_to_file)


def validate_schema(df: DataFrame, schema: DataFrame.schema) -> bool:
    """This function validate if loaded dataframe with correct schema
    :param df: dataframe on which will be schema validated
    :type df: DataFrame
    :param schema: schema used to validation
    :type schema: DataFrame.schema
    """
    return df.schema == schema
