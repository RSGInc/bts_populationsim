import requests
import os
import geopandas as gpd
import pandas as pd
import settings
from us import states
from io import BytesIO
from bs4 import BeautifulSoup
from utils import get_with_progress


def fetch(geo: str, year: int = settings.YEAR) -> gpd.GeoDataFrame:
    """
    Fetches geography files from the Census FTP server.

    Args:
        geo (str): The geography to fetch. Must be one of 'BG', 'TRACT', 'COUNTY', 'STATE', or 'PUMA'.
        year (int): The year to fetch data for.

    Returns:
        gpd.GeoDataFrame: The geography data.
    """
    
    
    assert isinstance(geo, str), f'Expected str, got {type(geo)}'
    
    url = f'https://www2.census.gov/geo/tiger/TIGER{year}/{geo}/'
    
    def check_soup(node):
        href = node.get('href')
        try:            
            is_zip = href.endswith('.zip')
            is_state = os.path.splitext(href)[0].split('_')[2] in settings.FIPS        
        except:
            return False
            
        if is_zip and is_state:
            return True
    
    def parse_soup(node):
        href = node.get('href')
        
        dlurl = url + '/' + href   
        fips = os.path.splitext(href)[0].split('_')[2]
        fpath = os.path.join(settings.RAW_DATA_DIR, 'shp', href)
        
        return fips, fpath, dlurl


    # Connect to the FTP server
    response = requests.get(url)
    
    # Get file URLs
    soup = BeautifulSoup(response.text, 'html.parser')
    zips = [parse_soup(node) for node in soup.find_all('a') if check_soup(node)]  
    
    if not os.path.exists(os.path.join(settings.RAW_DATA_DIR, 'shp')):
        os.mkdir(os.path.join(settings.RAW_DATA_DIR, 'shp'))
    
    # Parquet file path
    pq_path = os.path.join(settings.RAW_DATA_DIR, f'geography_{geo}.parquet')
    if os.path.exists(pq_path):
        print(f'Loading existing {geo} geography data')
        geo_df = gpd.read_parquet(pq_path)
    else:
        geo_df = gpd.GeoDataFrame()
    
    state_col = list(set(['STATEFP', 'STATEFP10']).intersection(geo_df.columns))
    assert len(state_col) <= 1, f'Expected 1 state column, got {len(state_col)}'
    if len(state_col) > 0:
        state_list = geo_df[state_col[0]]        
        assert isinstance(state_list, pd.Series), f'Expected pd.Series, got {type(state_list)}'
        state_list = state_list.unique()
    else:
        state_list = []  
     
    new_data = [geo_df]
    for fips_code, fpath, zurl in zips:        
        # First check if we need to fetch this state
        if fips_code in settings.FIPS and fips_code not in state_list:
            state_obj = states.lookup(fips_code)
            state_name = getattr(state_obj, 'name')
            
            if not os.path.exists(fpath):
                
                print(f'Downloading {state_name} {geo} geography data')                
                bytes_data = get_with_progress(zurl)
                assert isinstance(bytes_data, bytes), f'Expected bytes, got {type(bytes_data)}'
                
                with open(fpath, 'wb') as f:
                    f.write(bytes_data)
                    
                df = gpd.read_file(BytesIO(bytes_data))
            else:
                print(f'Loading cached {state_name} {geo} geography data')
                df = gpd.read_file(fpath)
                
            new_data.append(df)
                
    if len(new_data) > 1:
        geo_df = pd.concat(new_data, axis=0)
        geo_df.to_parquet(path=pq_path)
        
    names = dict(zip(geo_df.columns, geo_df.columns.str.replace('10|20', '', regex=True)))    
    geo_df.rename(columns=names, inplace=True)
    
    return gpd.GeoDataFrame(geo_df)


if __name__ == '__main__':
    GEO_BG = fetch('BG')
    GEO_PUMA = fetch('PUMA')