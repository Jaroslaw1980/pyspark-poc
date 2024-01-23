from logging import Logger

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.file_loader import find_files_in_path, load_data_from_file
from src.schemas import Schemas
from src.session import start_session
from src.utils import parse_arguments
from src.variables import OUTPUT_PATH, PATH_TO_DATA_FILES, path_to_users_data, path_to_transactions_data


class CodacApp:

    def __init__(self, logger: Logger) -> None:
        """The method gets spark session and logger
        :param logger: a Logger object
        :type logger: Logger
        """
        self.logger = logger

    def run(self, columns_to_drop, column, on, join_method, columns_to_rename, load_file_format, mode, save_file_format):
        """
        Method to run all flow process. Parameters comes from config.yaml
        """
        self.logger.info('Program is starting')

        args = parse_arguments()
        spark = start_session()

        find_files_in_path(PATH_TO_DATA_FILES, self.logger)

        users = load_data_from_file(spark, load_file_format, path_to_users_data,
                                    self.logger, Schemas.dataset_one_schema)
        transaction = load_data_from_file(spark, load_file_format, path_to_transactions_data,
                                          self.logger, Schemas.dataset_two_schema)

        users_dropped = self.drop_column(users, columns_to_drop)
        users_filtered = self.filter_data(users_dropped, column, args.countries)
        transaction_dropped = self.drop_column(transaction, columns_to_drop)
        df_joined = self.join_dfs(users_filtered, transaction_dropped, on, join_method)
        df_output = self.rename_column(df_joined, columns_to_rename)

        self.save_data(df_output, save_file_format, mode, file_path=OUTPUT_PATH)

        spark.stop()

        self.logger.info('Program is finished')

    def rename_column(self, df: DataFrame, column_to_rename: dict) -> DataFrame:
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

    def filter_data(self, df: DataFrame, column: str, filter_values: list) -> DataFrame:
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

    def drop_column(self, df: DataFrame, columns_to_drop: list) -> DataFrame:
        """Method used to drop columns from given dataframe
        :param df: dataframe on which columns will be dropped
        :type df: DataFrame
        :param columns_to_drop: list of columns that will be droped from dataframe
        :type columns_to_drop: list
        :return returns new dataframe without dropped columns
        :rtype pyspark.sql.Dataframe
        """
        # self.values = values
        self.logger.info(f"Droping {columns_to_drop} from dataframe")
        return df.drop(*columns_to_drop)

    def join_dfs(self, df1: DataFrame, df2: DataFrame, on: str, join_method: str) -> DataFrame:
        """Method used to join two dataframes on given column and join type
        :param df1: first dataframe to join
        :type df1: DataFrame
        :param df2: second dataframe to join
        :type: Dataframe
        :param on: name of the column on which two dataframes will be joined
        :type on: str
        :param join_method: type of the join for example: inner, outer, left
        :type join_method: str
        :return returns new dataframe from two joined dataframes
        :rtype pyspark.sql.Dataframe
        """
        self.logger.info("Joining two dataframes")
        return df1.join(df2, on, join_method)

    def save_data(self, df: DataFrame, file_format: str, mode: str, file_path: str) -> None:
        """Method used to save dataframe on given file path
        :param df: dataframe to save
        :type df: DataFrame
        :param file_format: format of the file to save to
        :type file_format: str
        :param mode: in which mode datafile will be saved
        :type mode: str
        :param file_path: file path to which dataframe will be saved
        :type file_path: str
        :return None
        """
        try:
            df.write.format(file_format).mode(mode).option("header", "true").save(file_path)
            self.logger.info(f"Dataframe saved to {file_path}")
        except IOError as error:
            self.logger.error(error)
