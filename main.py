from src.codac_app import CodacApp
from src.session import start_session
from src.utils import create_logger, parse_arguments, check_if_file_exists
from src.variables import OUTPUT_PATH
from src.schemas import Schemas


def main():
    """Main method to execute logic from CodacApp class"""

    # create logger, argument parser and spark session
    logger = create_logger()
    args = parse_arguments()
    spark = start_session()

    codac = CodacApp(spark, logger)

    if check_if_file_exists(args.file_one, args.file_two):
        df1 = codac.load_data(args.file_one, Schemas.dataset_one_schema)
        df1 = codac.drop_column(df1, ['first_name', 'last_name'])

        df2 = codac.load_data(args.file_two, Schemas.dataset_two_schema)
        df2 = codac.drop_column(df2, ['cc_n'])

        df3 = codac.join_dfs(df1, df2, on='id')
        df3 = codac.filter_data(df3, 'country', args.countries)
        column_name_mapping = {'id': 'client_identifier',
                               'btc_a': 'bitcoin_address',
                               'cc_t': 'credit_card_type'}
        df3 = codac.rename_column(df3, column_name_mapping)
        df3.show()
        codac.save_data(df3, OUTPUT_PATH)
    else:
        logger.error('There is no file to load under the paths')

    spark.stop()

    logger.info('Program is finished')


if __name__ == "__main__":
    main()
