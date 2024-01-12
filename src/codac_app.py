from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


class CodacApp:

    def __init__(self, spark: SparkSession, logger: Logger) -> None:
        """The method gets spark session and logger
        :param spark: a SparkSession object
        :type spark: SparkSession
        :param logger: a Logger object
        :type logger: Logger
        """
        self.spark = spark
        self.logger = logger

    def load_data(self, file_path: str, schema: StructType) -> DataFrame:
        """Merhod used to load data from csv file to dataframe
        :param file_path: path to file with data
        :type file_path: str
        :param schema: schema for data to load
        :type schema: StructType
        :return return dataframe containing data from loaded file
        :rtype pyspark.sql.Dataframe
        """
        file_format = file_path.split('.')[-1]
        self.logger.info(f"Loading file: {file_path}")
        try:
            df = self.spark.read.format(file_format) \
                .option("header", True) \
                .schema(schema) \
                .load(file_path)
            self.logger.info(f"{file_format} loaded with {str(df.count())} rows")
            return df
        except FileNotFoundError as error:
            self.logger.error(f"Problem with loading file, file doesn't exists")
            self.logger.error(error)
        except IOError as error:
            self.logger.error(f"Problem with loading {file_path}, IOError")
            self.logger.error(error)
        except Exception as error:
            self.logger.error(error)

    def rename_column(self, df: DataFrame, column_to_rename: dict) -> DataFrame:
        """Method rename sepecific column in dataframe using given dictionary
        :param df: dataframe on which renaming column will be proceded
        :type df: DataFrame
        :param column_to_rename: Dictionary with key as old name and value as new name of the column
        :type column_to_rename: dict
        :return Dataframe
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info(f"Renaming columns")
        for k, v in column_to_rename.items():
            df = df.withColumnRenamed(k, v)
        return df

    def filter_data(self, df: DataFrame, column: str, filter_values: list) -> DataFrame:
        """Method to filter specific column in dataframe. It uses list of values to filter after
        :param df: dataframe to perform filtering
        :type df: DataFrame
        :param column: column used to filter
        :type column: str
        :param filter_values: list of values used to filter on given column
        :type filter_values: list
        :return returns filtered dataframe
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info(f"Filtering {column} by {filter_values}")
        filtered = df.filter(col(column).isin(filter_values))
        return filtered

    def drop_column(self, df: DataFrame, values: list) -> DataFrame:
        """Method used to drop columns from given dataframe
        :param df: dataframe on which columns will be dropped
        :type df: Dataframe
        :param values: list of columns that will be droped from dataframe
        :type values: list
        :return returns new dataframe without dropped columns
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info(f"Droping {values} from dataframe")
        df = df.drop(*values)
        return df

    def join_dfs(self, df1: DataFrame, df2: DataFrame, on: str, how: str = 'inner') -> DataFrame:
        """Method used to join two dataframes on given column and join type
        :param df1: first dataframe that will be joined
        :type df1: Dataframe
        :param df2: second dataframe that will be joined
        :type df2: Dataframe
        :param on: name of the column on which two dataframes will be joined
        :type on: str
        :param how: type of the join for example: inner, outer, left
        :type how: str -> default value == 'inner'
        :return returns new dataframe from two joined dataframes
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info("Joining two dataframes")
        df = df1.join(df2, on, how)
        return df

    def save_data(self, df: DataFrame, file_path: str) -> None:
        """Method used to save dataframe on given file path
        :param df: dataframe that will be saved
        :type df: Dataframe
        :param file_path: file path to which dataframe will be saved
        :type file_path: str
        :return None
        """
        try:
            df.toPandas().to_csv(file_path, header=True)
            self.logger.info(f"Dataframe saved to {file_path}")
        except IOError as error:
            self.logger.error(f"Issue with saving dataframe")
            self.logger.error(error)
