import os
from logging import Logger

from pyspark.sql import DataFrame, SparkSession

import src.variables


def find_files_in_path(path: str, logger: Logger) -> None:
    """Function to find files in raw_data folder
    :param path: a path to raw_data folder
    :type path: str
    :param logger: object of logger
    :type logger: logging.Logger
    """
    logger.info('Checking for data files in data folder')
    data_files_in_path = os.listdir(path)
    data_files_filtered = sorted(list(filter(lambda file: file.endswith('csv'), data_files_in_path)))
    if not data_files_filtered:
        logger.error('Found zero data files')
        raise FileNotFoundError("No data files to load")
    elif len(data_files_filtered) == 2:
        logger.info('Found 2 data files, setting files names into variables')
        variable_mapping = {
            'users': data_files_filtered[0],
            'transactions': data_files_filtered[1]
        }
        src.variables.users = variable_mapping['users']
        src.variables.transactions = variable_mapping['transactions']
    else:
        logger.error('There are wrong number of files in the folder')
        raise ValueError(f"Expected two files, but found {len(data_files_filtered)} files")


def load_data_from_file(spark: SparkSession, load_file_format: str, save_file_path: str,
                        logger: Logger, schema: DataFrame.schema) -> DataFrame:
    """Function used to load data from raw data_file
    :param spark: a SparkSession object
    :type spark: SparkSession
    :param load_file_format: paramter for format on which dataframe should be loaded
    :type load_file_format: str
    :param save_file_path: file path to the raw data_file
    :type save_file_path: str
    :param logger: a Logger object
    :type logger: Logger
    :param schema: list of schemas from Schemas dataclass
    :type schema: DataFrame.schema
    :return loaded dataframe
    :rtype dataframe
    """
    logger.info(f"Loading data from the file path: {save_file_path}")
    df = spark.read.format(load_file_format).option("header", "true").schema(schema).load(save_file_path)
    logger.info('Dataframe loaded')
    return df
