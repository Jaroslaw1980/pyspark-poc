from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.file_loader import find_files_in_path, load_data_from_file
from src.utils import parse_arguments
from src.variables import OUTPUT_PATH, PATH_TO_DATA_FILES, PATH_FILE_ONE, PATH_FILE_TWO


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

    def run(self, values, column, on, columns_to_rename, file_format):
        """
        Method to run all flow process. Parameters comes from config.yaml
        """
        self.logger.info('Program is starting')
        args = parse_arguments()

        self.logger.info('Checking for data files in data folder')
        find_files_in_path(PATH_TO_DATA_FILES, self.logger)

        df_1 = load_data_from_file(self.spark, PATH_FILE_ONE, self.logger)
        df_2 = load_data_from_file(self.spark, PATH_FILE_TWO, self.logger)

        df_1_dropped = self.drop_column(df_1, values)
        df_filtered = self.filter_data(df_1_dropped, column, args.countries)
        df_2_dropped = self.drop_column(df_2, values)

        df_joined = self.join_dfs(df_filtered, df_2_dropped, on)
        df_output = self.rename_column(df_joined, columns_to_rename)
        self.save_data(df_output, file_format, file_path=OUTPUT_PATH)

        self.spark.stop()

        self.logger.info('Program is finished')

    def rename_column(self, df, column_to_rename: dict) -> DataFrame:
        """Method rename sepecific column in dataframe using given dictionary
        :param df: dataframe on which columms will be renamed
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

    def filter_data(self, df, column: str, filter_values: list) -> DataFrame:
        """Method to filter specific column in dataframe. It uses list of values to filter after
        :param df: dataframe to filter values
        :type df: DataFrame
        :param column: column used to filter
        :type column: str
        :param filter_values: list of values used to filter on given column
        :type filter_values: list
        :return returns filtered dataframe
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info(f"Filtering column: {column} by {filter_values}")
        return df.filter(col(column).isin(filter_values))

    def drop_column(self, df, values: list) -> DataFrame:
        """Method used to drop columns from given dataframe
        :param df: dataframe on which columns will be dropped
        :type df: DataFrame
        :param values: list of columns that will be droped from dataframe
        :type values: list
        :return returns new dataframe without dropped columns
        :rtype pyspark.sql.Dataframe
        """
        # self.values = values
        self.logger.info(f"Droping {values} from dataframe")
        return df.drop(*values)

    def join_dfs(self, df1, df2, on: str, how: str = 'inner') -> DataFrame:
        """Method used to join two dataframes on given column and join type
        :param df1: first dataframe to join
        :type df1: DataFrame
        :param df2: second dataframe to join
        :type: Dataframe
        :param on: name of the column on which two dataframes will be joined
        :type on: str
        :param how: type of the join for example: inner, outer, left
        :type how: str -> default value == 'inner'
        :return returns new dataframe from two joined dataframes
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info("Joining two dataframes")
        return df1.join(df2, on, how)

    def save_data(self, df, file_format: str, file_path: str) -> None:
        """Method used to save dataframe on given file path
        :param df: dataframe to save
        :type df: DataFrame
        :param file_format: format of the file to save to
        :type file_format: str
        :param file_path: file path to which dataframe will be saved
        :type file_path: str
        :return None
        """
        try:
            df.write.format(file_format).mode('overwrite').option("header", "true").save(file_path)
            self.logger.info(f"Dataframe saved to {file_path}")
        except IOError as error:
            self.logger.error(error)
