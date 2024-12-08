import os
import logging
from datetime import datetime

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