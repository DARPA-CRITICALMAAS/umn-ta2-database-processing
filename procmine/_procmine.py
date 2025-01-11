import os
import warnings
import polars as pl
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pl.MapWithoutReturnDtypeWarning)

from procmine import data, converting

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
        mode_map = data.check_mode(self.path_map)
        if mode_map == 'dir':
            raise ValueError("Attribute map cannot be a directory.",
                             "Please input a single attribute map.")
        
        self.map = data.load_data(self.path_map, mode_map).drop_nulls(subset=['corresponding_attribute_label'])

        # URI and record_id is required. If not available, ask user to fill in this information in the data dictionary
        list_cols = self.map['attribute_label'].to_list()
        if 'uri' not in list_cols:
            raise ValueError("Mapping file is missing the URI (data source) attribute.\n"
                             "This is required. Modify the data dictionary such that it has the URI attribute filled.")
        if 'record_id' not in list_cols:
            raise ValueError("Mapping file is missing the record id attribute.\n"
                             "This is required. Modify the data dictionary such that it has the record id attribute filled.")
        
        # Get record_id attribute label
        join_col = self.map.filter(pl.col('attribute_label') == 'record_id').item(0, 'corresponding_attribute_label')

        # Check data file exists and load data
        mode_data = data.check_mode(self.path_data)

        if data.check_exist(self.path_data) == 1:
            self.data = data.load_data(self.path_data, mode_data,
                                       join_col = join_col,
                                       list_files = self.map['file_name'].str.to_lowercase().to_list())
        else:
            raise ValueError("Unable to locate data file.",
                             f"Please check that the data {self.path_data} exists")

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
                .otherwise(pl.col('file_name').str.to_lowercase() + pl.lit(';') + pl.col('corresponding_attribute_label'))
        ).drop(
            ['corresponding_attribute_label', 'file_name']
        ).rename({'tmp': 'corresponding_attribute_label'})

        # Map labels based on mapping dictionary
        self.data, dict_literals = converting.label2label(pl_data=self.data, pl_label_map=self.map)

        # Append additional literals and add literals to the original data
        dict_literals['source_id'] = f"database::{dict_literals['uri']}"
        dict_literals['reference'] = {"document": {"uri": dict_literals['uri']}}
        dict_literals['created_by'] = "https://minmod.isi.edu/users/s/umn"
        dict_literals['modified_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        del dict_literals['uri']

        self.data = converting.add_attribute(self.data, dict_attributes=dict_literals)

        # Load_entity as dataframe
        self.entities = compile_entities(self.dir_entities, self.dict_entity_cols)
        logger.info(f"Entities dictionary has been created based on those available as CSV in {self.dir_entities}")

        # If crs available, convert crs to epsg
        if 'crs' in list(self.data.columns):
            crs_val = self.data['crs'].unique().to_list()

            dict_crs = {}
            for c in crs_val:
                epsg_val = converting.crs2epsg(crs_value=c)
                dict_crs[c] = epsg_val

            self.data = self.data.with_columns(pl.col('crs').replace(dict_crs))

        # Case 1: latitude and longitude present
        if 'latitude' in list(self.data.columns) and 'longitude' in list(self.data.columns):
            # Temporarily store those without any location information
            data_no_loc = self.data.filter(pl.col('longitude') == "", pl.col('latitude') == "").drop(['latitude', 'longitude'])
            self.data = self.data.filter(pl.col('longitude') != "", pl.col('latitude') != "")
            epsg_val = self.data['crs'].unique().to_list()[0]
            gpd_data = converting.non2geo(self.data, 
                                          str_lat_col='latitude', str_long_col='longitude',
                                          crs_val=epsg_val)
            
            self.data = pl.concat([converting.geo2non(gpd_data), data_no_loc], 
                                  how='diagonal')
        # Case 2: location present
        elif 'location' in list(self.data.columns):
            # Temporarily store those without any location information
            data_no_loc = self.data.filter(pl.col('location') == "")
            self.data = self.data.filter(pl.col('location') != "")
            epsg_val = self.data['crs'].unique().to_list()[0]
            gpd_data = converting.non2geo(self.data, 
                                          str_geo_col='location',
                                          crs_val=epsg_val)
            
            self.data = pl.concat([converting.geo2non(gpd_data), data_no_loc], 
                                  how='diagonal')
        
        # Maps PGE and REE commodity to list of pre-defined commodities
        path_commod_groups = os.path.join(self.dir_entities, '_commod_groups.pkl')
        if data.check_exist(path_commod_groups) != -1:
            dict_commod_groups = data.load_data(path_commod_groups, '.pkl')
        self.data = self.data.with_columns(
            pl.col('commodity').str.replace_many(dict_commod_groups)
        )       # TODO: check

        # Pop necessary columns
        col_to_pop = set(list(self.data.columns)) & {'aliases', 'state_or_province', 'country', 'commodity', 'deposit_type'}
        self.data = self.data.with_columns(
            pl.col(col_to_pop).str.replace_all(r"\s*[\|,;]\s*", ";").str.split(';')
        )

        # Rename crs column to epsg (for conversion purpose)
        if 'crs' in list(self.data.columns):
            self.data = self.data.rename({'crs': 'epsg'})

        # Convert to schema format
        self.data = converting.data2schema(pl_input=self.data, dict_all_entities=self.entities)

    def save_output(self,
                    save_format: str='json') -> None:
        
        self.path_output = os.path.join(self.dir_output, self.file_output)

        try:
            data.save_data(self.data, path_save=self.path_output, save_format=save_format)
            logger.info(f"Data saved to {self.path_output}.{save_format}")

        except:
            logger.warning("Unacceptable save format. Defaulting to pickle.")
            data.save_data(self.data, path_save=self.path_output, save_format='pickle')