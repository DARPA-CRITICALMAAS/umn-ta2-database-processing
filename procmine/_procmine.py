import os
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from procmine import data
from procmine import converting

from procmine._utils import (
    DefaultLogger
)

logger = DefaultLogger()
logger.configure("INFO")

class ProcMine:
    def __init__(self,
                 path_data: str,
                 path_map: str=None,
                 dir_output:str=None,
                 file_output:str=None,
                 verbose: bool=False) -> None:
        
        self.path_data = path_data
        self.path_map = path_map
        self.dir_output = dir_output
        self.file_output = file_output

        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def process(self):
        # Loading data
        self.load_data()
        self.load_map()

        # Load_entity

        # Converting to map dictionary
        converting.non2dict(pl_data=self.map,
                            key_col='key')
        
        # # Save processed output
        # self.save_output()

    def load_data(self):
        path_file = self.path_data

        mode_data = data.check_mode(path_file)

        try:
            self.data = data.load_data(path_file, mode_data)
            return 1
        
        except:
            raise ValueError("Unable to locate data file.",
                             f"Please check that the data {self.path_data} exists")

    def load_map(self) -> None:
        path_file = self.path_map

        if not path_file:
            if not path_file:
                logger.warning("Attribute map not provided. Result may be incorrect.")
                # TODO: Add an column identification pipeline tool
            return 0

        mode_map = data.check_mode(path_file)
    
        if self.mode_map == 'dir':
            raise ValueError("Attribute map cannot be a directory.",
                             "Please input a single attribute map.")
        
        self.map = data.load_data(path_file, mode_map)

    def save_output(self,
                    save_format: str='json') -> None:
        
        path_output_dir = self.dir_output
        path_output_file = self.file_output

        if not path_output_dir:
            logger.info("Saving to default path: ./output")
            path_output_dir = './output'
            data.check_directory_pathcheck(path_directory=path_output_dir)
        
        if not path_output_file:
            file_name, _ = os.path.splitext(os.path.basename(self.path_data))

            logger.info(f"Saving as default file:{file_name}output.json")
            path_output_file = file_name

        if data.check_mode(path_output_dir) != 'dir':
            logger.warning("Inputted output directory name is not a directory path.\nSaving to default path: ./output")
            path_output_dir = './output'

        self.path_output = os.path.join(path_output_dir, path_output_file)

        try:
            data.save_data(self.data, path_save=self.path_output, save_format=save_format)

        except:
            # TODO: Test if enters here when unsupported save format is given
            logger.warning("Unacceptable save format. Defaulting to pickle.")
            data.save_data(self.data, path_save=self.path_output, save_format='pickle')