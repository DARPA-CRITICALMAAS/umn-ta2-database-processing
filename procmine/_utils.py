import os
import logging
from typing import List
from datetime import datetime

import polars as pl
from procmine import data

class DefaultLogger:
    def __init__(self):
        self.logger = logging.getLogger('ProcMine')
        self.log_dir = './logs/'

    def configure(self, 
                  level):
        self.set_level(level)
        self._add_handler()

    def set_level(self, 
                  level):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if level in levels:
            self.logger.setLevel(level)

    def info(self, 
             message:str):
        self.logger.info(message)

    def warning(self, 
                message:str):
        self.logger.warning(message)
        
    def error(self, 
              message:str):
        self.logger.error(message)

    def _add_handler(self):
        # Initiating streamhandler i.e., terminal
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        self.logger.addHandler(sh)

        # Initiating filehandler i.e., log file
        data.check_directory_path(path_directory='./logs/')  # Creating log folder if not exist
        fh = logging.FileHandler(f'./logs/procmine_{datetime.timestamp(datetime.now())}.log')
        fh.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        self.logger.addHandler(fh)

def compile_entities(dir_entities: str) -> List[pl.DataFrame, List[str]]:
    """
    Before calling ProcMine, recompile the entities folder
    (In case there has been any updates to the entities)

    # TODO: fill info
    Arguments
    : dir_entities

    Return
    """

    if data.check_exist(dir_entities) < 0:
        raise ValueError("Unable to locate entities directory")
    
    if data.check_mode(dir_entities) != 'dir':
        raise ValueError("This is not an entity directory")

    list_pl_entities = []
    list_entities = []

    for filename in os.listdir(dir_entities):
        if data.check_mode(filename) != '.csv':
            continue

        path_entity = os.path.join(dir_entities, filename)
        pl_data = data.load_data(path_entity)

        entity_type = data.return_basename(filename)

        if 'minmod_id' not in list(pl_data.columns):
            continue

        pl_data = pl_data.with_columns(
            entity_type = pl.lit(entity_type)
        )
        list_pl_entities.append(pl_data)
        list_entities.append(entity_type)

    pl_entities = pl.concat(
        list_pl_entities,
        how='diagonal'
    )

    return pl_entities, list_entities