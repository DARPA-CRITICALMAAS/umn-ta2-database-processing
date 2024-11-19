import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from procmine import data
from procmine._utils import (
    DefaultLogger
)
import procmine._save_utils as save_utils

logger = DefaultLogger()
logger.configure("INFO")

class ProcMine:
    def __init__(self,
                 verbose: bool=False
                 ):
        
        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def load_data(self,
                  file_path: str=None):
        
        try:
            self.data = data.load_data(file_path)
            return 1
        
        except:
            logger.warning("Unable to locate data file. Aborting.")
            return -1

    def load_map(self,
                 file_path: str=None):
        
        try:
            if not file_path:
                logger.warning("Attribute map not provided. Result may be incorrect.")
            return 0

        except:
            self.map = data.load_data(file_path)
            return 1