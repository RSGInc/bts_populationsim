import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter, Retry
import json
from tqdm import tqdm
from itertools import islice
from bs4 import BeautifulSoup
import os
import us

from setup_inputs import settings


def format_geoids(target_df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    This function formats the geoids in a DataFrame to the correct length and format.
    This is done using integer summation rather than string concatenation for performance over large dataframe.

    Args:
        df (pd.DataFrame): the DataFrame to format

    Returns:
        pd.DataFrame: the formatted DataFrame
    """
    
    cols = list(set(target_df.columns).intersection(settings.GEOID_LEN.keys()))
    
    assert len(cols) > 0, 'No geoid columns found in the DataFrame'
    assert isinstance(target_df, pd.DataFrame), 'Input is not a DataFrame'
    assert isinstance(cols, list), 'Input is not a set'
    
    df = target_df[cols].copy()
    
    # Can use this to debug column formatting
    # df[[x + '_old' for x in cols]] = df[cols]
    
    for geoid, digits in settings.GEOID_LEN.items():
        if geoid in df.columns:
            if verbose:
                print(f'Formatting {geoid} GEOID field')
            
            # Ensure that the geoid is an integer
            if df[geoid].dtype != 'int64' or df[geoid].dtype != 'int32':
                df[geoid] = df[geoid].astype(np.int64)
            
            # Pre formatting the geoid to be consistent with itself           
            df[geoid] = df[geoid] % (10 ** digits)
            
            # Concatenate the geoid parts ensuring that only the appropriate last n-digits are used
            new_id = 0
            full_digits = sum([settings.GEOID_LEN[x] for x in settings.GEOID_STRUCTURE[geoid]])            
            for i, part_col in enumerate(settings.GEOID_STRUCTURE[geoid]):
                part_digits = settings.GEOID_LEN[part_col]
                full_digits += part_digits
                
                assert df[part_col].isna().sum() == 0, f'Geoid part {part_col} has missing values'
                
                part = df[part_col] % (10 ** part_digits)
                
                # If not the first GEOID part (i.e., state), need to add trailing zeroes for the part to be added
                if i > 0:                    
                    new_id *= (10 ** part_digits)

                new_id += part                
            
            # Send the new geoid back to the DataFrame
            df[geoid] = new_id

    target_df.update(df)
    
    return target_df              

def get_with_progress(url: str) -> bytes:
    """
    This function gets data from a URL with a progress bar.

    Args:
        url (str): The URL to get data from

    Returns:
        str: The data string from the URL
    """

    try:
        s = requests.Session()
        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        s.mount('https://', HTTPAdapter(max_retries=retries))
        response = requests.get(url, stream=True)
    except:
        raise Exception(f'Error fetching data from {url}, check your internet connection or maybe reduce the request size?')
    
    # Check the response status code        
    assert response.status_code == 200, f'{response.status_code} error: {response.text}'
    
    raw_data = b''        
    total = int(response.headers.get('Content-Length', 0))
    with tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024) as pbar:            
        for chunk in response.iter_content(1000000):
            raw_data += chunk
            pbar.update(len(chunk))
            
    # The request was successful, so parse the JSON response
    if 'utf-8' in response.headers.get('Content-Type', '').lower():
        raw_data = json.loads(raw_data.decode('utf-8'))
    
    return raw_data

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch
        
def parse_census_ftp(url: str, cache_dir: str, data_type: str) -> list:
    """
    This function parses the Census' HTTPS FTP server for data files and 
    returns a list of tuples containing the file name, download URL,
    file path and level (e.g., hh or per, bg or tracts, etc.)

    Args:
        url (str): The URL to the Census HTTPS FTP server.
        cache_dir (str): The directory to cache the data files.
        data_type (str): The type of data to download (geography or pums).

    Raises:
        Exception: If the data type is not geography or pums.
        Exception: If the file name is not in the expected format.

    Returns:
        list: A list of tuples containing the file name, download URL,
    """
    
    data_type = data_type.lower()
    assert data_type.lower() in ['geography', 'pums'], f'Invalid data type {data_type}'
    
    def check_soup(node: BeautifulSoup) -> bool:
        """
        This function checks if the node is a valid file to download.

        Args:
            node (str): The beautiful soup node to check.

        Raises:
            Exception: The file name is not in the expected format.

        Returns:
            bool: True if the file is valid, False otherwise.
            
        """
                
        try:            
            href = node.get('href')
            assert isinstance(href, str), f'Invalid href {href}'

            is_zip = href.endswith('.zip')
            if data_type == 'geography':
                is_state = os.path.splitext(href)[0].split('_')[2] in settings.FIPS
            elif data_type == 'pums':
                is_state = os.path.splitext(href)[0][-2:].upper() in settings.STATES_AND_TERRITORIES
            else:
                raise Exception(f'Invalid data type {data_type}')
        except:
            return False
            
        if is_zip and is_state:
            return True
        else:
            return False
    
    def parse_soup(node: BeautifulSoup) -> tuple:
        """
        This function parses the beautiful soup node and returns a tuple

        Args:
            node (str): The beautiful soup node to parse.

        Raises:
            Exception: The file name is not in the expected format.

        Returns:
            tuple: A tuple containing the fips code, file path, download URL, and level
        """
        href = node.get('href')
        assert isinstance(href, str), f'Invalid href {href}'
        
        dlurl = url + '/' + href
        file_name = os.path.splitext(href)[0]        
        level = None
        if data_type == 'geography':
            fips, level = file_name.split('_')[2:]
        else:            
            suffix = file_name.split('_')[1]
            fips = getattr(us.states.lookup(suffix[-2:].upper()), 'fips')            
            if 'h' in suffix[0]:
                level = 'hh'
            elif 'p' in file_name:
                level = 'per'
            else:
                raise Exception(f'Invalid file name {file_name}, expected csv_h[state] or csv_p[state]')
            
        if os.path.isabs(cache_dir):
            fpath = os.path.join(cache_dir, href)
        else:
            fpath = os.path.join(settings.RAW_DATA_DIR, cache_dir, href)
        
        return fips, fpath, dlurl, level

    # Connect to the FTP server
    response = requests.get(url)
    
    # Get file URLs
    soup = BeautifulSoup(response.text, 'html.parser')
    zips = [parse_soup(node) for node in soup.find_all('a') if check_soup(node)]
    
    return zips