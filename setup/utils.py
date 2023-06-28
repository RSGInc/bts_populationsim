import pandas as pd
import requests
import json
from tqdm import tqdm
import settings

def format_geoids(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function formats the geoids in a DataFrame to the correct length and format.

    Args:
        df (pd.DataFrame): the DataFrame to format

    Returns:
        pd.DataFrame: the formatted DataFrame
    """
    for geoid, digits in settings.GEOID_LEN.items():        
        full_digits = sum([settings.GEOID_LEN[x] for x in settings.GEOID_STRUCTURE[geoid]])        
        try:
            is_formatted = (df[geoid].apply(len) == full_digits).all()
        except:
            is_formatted = False
        
        if geoid in df.columns and not is_formatted:
            df[geoid] = df[geoid].str.zfill(digits)
            
            # Concatenate the geoid parts ensuring that only the appropriate last n-digits are used
            parts = []           
            for part in settings.GEOID_STRUCTURE[geoid]:
                part_digits = settings.GEOID_LEN[part]
                assert df[part].isna().sum() == 0, f'Geoid part {part} has missing values'
                parts.append(df[part].str[-part_digits:].astype(str))
            
            df[geoid] = pd.concat(parts, axis=1).sum(axis=1)
            # df[geoid] = df[settings.GEOID_STRUCTURE[geoid]].sum(axis=1)
        
    return df            
    

def get_with_progress(url: str) -> bytes:
    """
    This function gets data from a URL with a progress bar.

    Args:
        url (str): The URL to get data from

    Returns:
        str: The data string from the URL
    """

    response = requests.get(url, stream=True)
    
    # Check the response status code        
    assert response.status_code == 200, f'{response.status_code} error: {response.text}'
    
    raw_data = b''        
    total = int(response.headers.get('Content-Length', 0))
    with tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024) as pbar:            
        for chunk in response.iter_content(1024):
            raw_data += chunk
            pbar.update(len(chunk))
            
    # The request was successful, so parse the JSON response
    if 'utf-8' in response.headers.get('Content-Type', '').lower():
        raw_data = json.loads(raw_data.decode('utf-8'))
    
    return raw_data