import requests
import polars as pl
import geopandas as gpd

from pyproj import CRS
from bs4 import BeautifulSoup as bs

pl.Config.set_fmt_float("full") # Convert scientific notation to float

def crs2crs(data: gpd.GeoDataFrame, 
            target_crs: str) -> gpd.GeoDataFrame:
    """
    Converts CRS from one type to another

    # TODO: Fill out the information
    Arguments
    : data:
    : target_crs:

    Return
    """
    target_epsg = crs2epsg(target_crs)
    data = data.to_crs(target_epsg)

    return data

def crs2epsg(crs_value:str) -> str:
    """
    Converts CRS to EPSG
    Searchs on epsg.io based on crs value name

    # TODO: Fill out the information
    Arguments
    : crs_value

    Return
    """
    # If it is already in EPSG, return crs
    if 'epsg' in crs_value.lower():
        return crs_value
    
    page = requests.get(f"https://epsg.io/?q={crs_value}%20%20kind%3AGEOGCRS")
    soup = bs(page.content, "html.parser")

    job_elements = soup.find_all("h4")
    if len(job_elements) == 1: 
        return 'EPSG:' + job_elements[0].find("a")['href'].strip().lstrip('/')
        
    for i in job_elements:
        a_object = i.find("a")
        if a_object.text.strip() == crs_value:
            return 'EPSG:' + a_object['href'].strip().lstrip('/')
        
def check_crs_range(epsg_value:str,
                    latitude:float, longitude:float) -> int:
    """
    Checks if latitude and longitude value is within the accetable range of the corresponding EPSG

    # TODO: Fill out the information
    Arguments
    : epsg_value: 
    : latitude:
    : longitude: 

    Return
    """
    _, numval = epsg_value.split(':')
    lat_min, long_min, lat_max, long_max = CRS.from_user_input(int(numval)).area_of_use.bounds

    if (lat_min <= latitude <= lat_max) and (long_min <= longitude <= long_max):
        return 1
    
    return -1