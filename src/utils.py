import argparse
import logging

from logging.handlers import RotatingFileHandler
from pyspark.sql import DataFrame

from src.variables import path_file_one, path_file_two, LOGGER_NAME, LOGS_PATH


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
                        default=path_file_one)
    parser.add_argument('--file_two', type=str, help='Path to file two',
                        default=path_file_two)
    parser.add_argument('--countries', type=str, nargs='+', help='Countries to filter',
                        default=['Netherlands', 'United Kingdom'])

    args = parser.parse_args()
    return args


def validate_schema(df: DataFrame, schema: DataFrame.schema) -> bool:
    """This function validate if loaded dataframe with correct schema
    :param df: dataframe on which will be schema validated
    :type df: DataFrame
    :param schema: schema used to validation
    :type schema: DataFrame.schema
    :return boolean value of comparing two schemas
    :rtype bool
    """
    return df.schema == schema
