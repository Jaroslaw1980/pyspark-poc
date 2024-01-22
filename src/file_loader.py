import os
from logging import Logger

from pyspark.sql import DataFrame, SparkSession

import src.variables
from src.schemas import Schemas
from src.utils import validate_schema


def find_files_in_path(path: str, logger: Logger) -> None:
    """Function to find files in raw_data folder if the path won't work
    :param path: a path to raw_data folder
    :type path: str
    :param logger: object of logger
    :type logger: logging.Logger
    """
    files = os.listdir(path)
    data_files = [file for file in files if file.endswith('csv')]
    if len(data_files) == 0:
        logger.error('Found zero files')
        raise FileNotFoundError
    elif len(data_files) == 2:
        logger.info('Found 2 files, setting files names into variables')
        src.variables.file_one = data_files[0]
        src.variables.file_two = data_files[1]
    else:
        logger.error('There are wrong number of files in the folder')
        raise Exception


def load_data_from_file(spark: SparkSession, file_path: str, logger: Logger, schemas) -> DataFrame:
    """Function used to load data from raw data_file
    :param spark: a SparkSession object
    :type spark: SparkSession
    :param file_path: file path to the raw data_file
    :type file_path: str
    :param logger: a Logger object
    :type logger: Logger
    :return loaded dataframe
    :rtype dataframe
    """
    logger.info(f"Loading data from the file: {file_path}")
    df = spark.read.format('csv').option("header", "true").load(file_path)
    # schemas = [Schemas.dataset_one_schema, Schemas.dataset_two_schema]
    logger.info(f"Finding correct schema for: {file_path}")
    try:
        for schema in schemas:
            if validate_schema(df, schema):
                logger.info(f"Correct schema for {df} is {schema} is found")
                break
            else:
                logger.error('There is no correct schema for dataframe')
    except logger.error('Something is wrong with Dataframe'):
        raise Exception
    logger.info('Dataframe loaded')
    return df
