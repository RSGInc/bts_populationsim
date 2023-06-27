import pandas as pd
import re
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



def aggregate_pums_fields(pums_fields_df: pd.DataFrame) -> dict:
    """
    This function automatically pulls PUMS fields from the control.csv file into a list of field names.
    Not ideal, best to simply hardcode the list of fields.

    Args:
        pums_fields_df (pd.DataFrame): The PUMS control.csv file

    Returns:
        list: The list of PUMS field names
    """
    
    # regexer = re.compile(r'\[(.*?)\]')    
    regexer = re.compile(r'\.(.*?) ')
    def regexfunc(x):
        try:
            res = regexer.search(x)
            if res:
                return res.group(1).strip('\"').strip('\'')
        except:
            return None
        
    pums_fields_df = pums_fields_df[~pums_fields_df.target.str.startswith('#')]    
    exp = pums_fields_df.expression
    fields = exp.apply(regexfunc)
    fields = {x: int for x in fields if x != 'SERIALNO'}
    
    return fields
    
def aggregate_acs_fields(acs_fields_df: pd.DataFrame) -> tuple:    
    """
    This function aggregates ACS fields into a dictionary of lists, 
    where the keys are the aggregated field names and the values are the ACS fields.

    Args:
        path (str): path to the field aggregator csv

    Returns:
        list[dict, dict, dict, list]: [field_agg, geo_levels, fields, tables]
            field_agg is a dictionary of lists, where the keys are the aggregated field names and the values are the ACS fields.
            geo_fields is a dictionary of dictionaries, where the keys are the geographies and the values are dictionaries of fields and their data types.
            tables is a list of tables.        
    """
    acs_fields_df = acs_fields_df[~acs_fields_df['control_field'].str.strip().replace('', None).isna()]
    field_agg = acs_fields_df.groupby(['geography', 'control_field'])['field'].apply(list).to_dict()    
    tables = acs_fields_df.group.unique().tolist()
    geo_fields = acs_fields_df.groupby('geography').apply(lambda x: dict(zip(x['field'], x['type']))).to_dict()
    control_fields = acs_fields_df.control_field.unique().tolist()
    #geo_levels = acs_fields_df.groupby('geography')['field'].apply(list).to_dict()    
    # fields = list(chain(*field_agg.values()))
    # fields = {x: int for x in fields}
    
    return (field_agg, geo_fields, control_fields, tables)

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