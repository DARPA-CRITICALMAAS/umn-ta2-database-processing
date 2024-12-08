# import copy
# import pytest
# from procmine import ProcMine
# import importlib.util

# processor = ProcMine(path_data='/users/2/pyo00005/HOME/CriticalMAAS/rdbms_all', 
#                      path_map='/home/yaoyi/pyo00005/CriticalMAAS/RAW_DATA/http:__www.oregongeology.org/oregon_mapfile.csv')

# print(processor.load_data())
# print(processor.check_entity_dir())

# # Pytest Sample
# def inc(x):
#     return x + 1

# def test_answer():
#     assert inc(3) == 5

import geopandas as gpd
import pandas as pd
from shapely import wkt

df = gpd.read_file('/users/2/pyo00005/HOME/CriticalMAAS/michigan.geojson', driver='GeoJSON')
df['crs'] = str(df.crs)
df['location'] = df['geometry'].apply(wkt.dumps)
df = df.drop('geometry', axis=1)

print(df)
print(df.columns)