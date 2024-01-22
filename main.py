import yaml

from src.codac_app import CodacApp
from src.utils import create_logger

if __name__ == "__main__":
    with open("config.yaml", "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    logger = create_logger()
    codac = CodacApp(logger)
    codac.run(values=config['drop_columns'],
              column=config['filter_on_column'],
              on=config['join_on_column'],
              columns_to_rename=config['rename_columns'],
              file_format=config['file_format'])
