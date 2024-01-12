import argparse
import logging
import os.path
from logging.handlers import RotatingFileHandler

from src.variables import PATH_FILE_ONE, PATH_FILE_TWO, LOGGER_NAME


def create_logger() -> logging.Logger:
    """Initialize rotating logger
    :return Logger
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    log_handler = logging.handlers.RotatingFileHandler(
        filename=r'C:\Projects\codac\logs\codac.log',
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
    parser.add_argument('--countries', type=str, help='Countries to filter',
                        default=['Netherland', 'United Kingdom'])

    args = parser.parse_args()
    return args


def check_if_file_exists(file_one: str, file_two: str) -> bool:
    """This function checks if there are files under the path to load
    :param file_one: path to a dataset file_one
    :type file_one str
    :param file_two: path to dataset file_two
    :type file_two str
    :return True of False depending on the files exists
    :rtype Bool
    """
    return os.path.exists(file_one) and os.path.exists(file_two)
