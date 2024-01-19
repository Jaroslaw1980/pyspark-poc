from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from src.utils import parse_arguments, check_if_file_exists, validate_schema, check_files_in_data_folder
from src.schemas import Schemas
from src.variables import OUTPUT_PATH, PATH_TO_DATA_FOLDER


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

        df_1 = self.load_data(file_path=args.file_one, schema=Schemas.dataset_one_schema)
        df_2 = self.load_data(file_path=args.file_two, schema=Schemas.dataset_two_schema)

        df_1_dropped = self.drop_column(df_1, values)
        df_filtered = self.filter_data(df_1_dropped, column, args.countries)
        df_2_dropped = self.drop_column(df_2, values)

        df_joined = self.join_dfs(df_filtered, df_2_dropped, on)
        df_output = self.rename_column(df_joined, columns_to_rename)
        self.save_data(df_output, file_format, file_path=OUTPUT_PATH)

        self.spark.stop()

        self.logger.info('Program is finished')

    def load_data(self, file_path: str, schema: StructType) -> DataFrame:
        """Method used to load data from csv file to dataframe
        :param file_path: path to file with data
        :type file_path: str
        :param schema: schema for data to load
        :type schema: StructType
        :return dataframe containing data from loaded file
        :rtype pyspark.sql.Dataframe
        """

        self.logger.info(f'Checking if file exists in the {file_path}')
        if not check_if_file_exists(path_to_file=file_path):
            self.logger.error(f'File in path: {file_path} do not exists')
            raise FileNotFoundError
        self.logger.info(f"Loading file: {file_path}")
        try:
            df = self.spark.read.format('csv') \
                .option("header", True) \
                .load(file_path)
            self.logger.info(f'Validating schema for {file_path}')
            if not validate_schema(df, schema):
                self.logger.error(f'{schema} for {file_path} is incorrect')
            self.logger.info(f"'csv loaded with {str(df.count())} rows")
            return df
        except IOError:
            self.logger.error(f"Problem with loading {file_path}, IOError")
        except Exception as error:
            self.logger.error(error)

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
            self.logger.error(f"Issue with saving dataframe")
            self.logger.error(error)
