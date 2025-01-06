import os
import warnings
import polars as pl
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pl.MapWithoutReturnDtypeWarning)

from procmine import data, converting, processing

from procmine._utils import (
    DefaultLogger,
    compile_entities,
)

logger = DefaultLogger()
logger.configure("INFO")

class ProcMine:
    def __init__(self,
                 path_data: str,
                 path_map: str=None,
                 dir_output:str=None,
                 file_output:str=None,
                 dir_entities:str='./procmine/_entities',
                 verbose: bool=False) -> None:
        
        self.path_data = path_data
        self.path_map = path_map
        self.dir_output = dir_output
        self.file_output = file_output

        self.dir_entities = dir_entities

        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def prepare_data_paths(self):
        # Check data file exists and load data
        mode_data = data.check_mode(self.path_data)

        if data.check_exist(self.path_data) == 1:
            self.data = data.load_data(self.path_data, mode_data)
        else:
            raise ValueError("Unable to locate data file.",
                             f"Please check that the data {self.path_data} exists")

        # Check map file exists else direct to map assumption
        if not self.path_map or data.check_exist(self.path_map) != 1:
            logger.warning("Attribute map not provided. Result may be incorrect.")
            # TODO: Add an column identification pipeline tool
        else:
            mode_map = data.check_mode(self.path_map)
            if mode_map == 'dir':
                raise ValueError("Attribute map cannot be a directory.",
                                "Please input a single attribute map.")
            
            self.map = data.load_data(self.path_map, mode_map).drop_nulls(subset=['corresponding_attribute_label'])

        # Check output directory and create output directory if not exist
        if self.dir_output and data.check_mode(self.dir_output) == 'dir':
            pass
        else:
            self.dir_output = './output/'
            logger.info("Saving to default path: ./output")
        data.check_directory_path(path_directory=self.dir_output)    

        # Check entity directory exists
        if data.check_directory_path(path_directory=self.dir_entities, bool_create=False) == -1:
            raise ValueError("Unable to locate entities directory.")
        bool_entity_update = False
        
        # Log entities directory update
        if bool_entity_update:
            logger.info(f"Updated entities directory in {self.dir_entities}")

        # Load dictionary that identifies selected columns for each entity type
        path_entities_mapfile = os.path.join(self.dir_entities, '_selected_cols.pkl')
        if data.check_exist(path_entities_mapfile) != -1:
            self.dict_entity_cols = data.load_data(path_entities_mapfile, '.pkl')
        else:
            raise ValueError(f"_selected_cols.pkl file cannot be found in {self.dir_entities} folder."
                             "Please move the '_selected_cols.pkl' file to the appropriate folder")

    def process(self):
        # Combine filename to corresponding attribute label
        self.map = self.map.with_columns(
            tmp = pl.when(pl.col('file_name').is_null())
                .then(pl.col('corresponding_attribute_label'))
                .otherwise(pl.col('file_name') + pl.lit(';') + pl.col('corresponding_attribute_label'))
        ).drop(
            ['corresponding_attribute_label', 'file_name']
        ).rename({'tmp': 'corresponding_attribute_label'})
        
        # TODO: Raise value error if mapping file does not have URI information
        # if not self.map.filter(pl.col('attribute_label') == 'uri').shape[0] == 0:
        #     raise ValueError(f"Mapping file is missing the URI (data source) attribute."
        #                      "This is required. Modify the data dictionary such that it has the URI attribute filled.")

        # Map labels based on mapping dictionary
        self.data, dict_literals = converting.label2label(pl_data=self.data, pl_label_map=self.map)

        # Append additional literals and add literals to the original data
        # dict_literals['source'] = "UMN Matching System-ProcMinev2"
        dict_literals['source_id'] = f"database::{dict_literals['uri']}"
        dict_literals['reference'] = {"document": {"uri": dict_literals['uri']}}
        dict_literals['created_by'] = "https://minmod.isi.edu/users/s/umn"
        dict_literals['modified_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        del dict_literals['uri']

        self.data = processing.add_attribute(self.data, dict_attributes=dict_literals)

        # Load_entity as dataframe
        self.entities = compile_entities(self.dir_entities, self.dict_entity_cols)
        logger.info(f"Entities dictionary has been created based on those available as CSV in {self.dir_entities}")
        
        # Convert to schema format
        self.data = converting.data2schema(pl_data=self.data, dict_all_entities=self.entities)

        # Append datetime information
        # current_datetime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        # self.data = processing.add_attribute(self.data, attribute='modified_at', value=current_datetime)

        # # Save processed output
        # self.save_output()

    def save_output(self,
                    save_format: str='json') -> None:
        
        if not path_output_file:
            file_name = data.return_basename(self.path_data)
            # file_name, _ = os.path.splitext(os.path.basename(self.path_data))

            logger.info(f"Saving as default file:{file_name}.json")
            path_output_file = file_name

        self.path_output = os.path.join(self.dir_output, self.file_output)

        try:
            data.save_data(self.data, path_save=self.path_output, save_format=save_format)

        except:
            # TODO: Test if enters here when unsupported save format is given
            logger.warning("Unacceptable save format. Defaulting to pickle.")
            data.save_data(self.data, path_save=self.path_output, save_format='pickle')