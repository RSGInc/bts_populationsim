import us
import os
import numpy as np
import pandas as pd
import zipfile
from io import BytesIO

from setup_inputs import settings
from setup_inputs.utils import get_with_progress, batched, parse_census_ftp


def api_get(data_type: str, state_fips: int|str|list, geo: str, field_dtypes: dict) -> pd.DataFrame:
    """
    Fetches data from the Census API and returns a pandas DataFrame.

    Args:
        data_type (str): The data type to fetch. Must be either "PUMS" or "ACS".
        state_fips (int): The FIPS code for the state to fetch data for.
        geo_fields (dict): A dictionary of geographies and their fields.
        field_dtypes (dict): A dictionary of fields and their data types.

    Returns:
        pd.DataFrame: The data fetched from the Census API.
    """
    
    assert data_type in ['PUMS', 'ACS'], 'data_type must be either "PUMS" or "ACS"'
    year = settings.YEAR
    acs_type = settings.ACS_TYPE  
    key = settings.CENSUS_API_KEY        
    fields = list(field_dtypes.keys())
    state_fips = ','.join(state_fips) if isinstance(state_fips, list) else state_fips
    
    # Max field request is 50, so we need to split the fields into chunks of 40
    chunk_size = 40 if data_type == 'ACS' else 3
    geo_map = {'BG': 'block%20group:*', 'TRACT': 'tract:*', 'COUNTY': 'county:*', 'STATE': f'state:{state_fips}', 'PUMA': 'public%20use%20microdata%20area:*'}
    nested_geo_map = {'BG': 'TRACT', 'TRACT': 'COUNTY', 'COUNTY': 'STATE', 'PUMA': 'STATE', 'STATE': None}
    
    # If there is a list of geographies, loop through them
    bg_str = f'&for={geo_map[geo]}'
    next_geo = nested_geo_map[geo]
    # Construct the nested geography string
    while next_geo is not None:
        bg_str += f'&in={geo_map[next_geo]}'
        next_geo = nested_geo_map[next_geo]
        
    df = None
    for i in range(0, len(fields), chunk_size):        
        to_iter = min(i + chunk_size, len(fields))
        print(f'Fetching fields {i} to {to_iter} of {len(fields)}')
        field_chunk = fields[i:i + chunk_size]
        # Assure that SERIALNO or SPORDER are included in PUMS data requests for merging
        if data_type == 'PUMS':
            for x in ['SERIALNO', 'SPORDER']:
                if x in fields and x not in field_chunk:
                    field_chunk.append(x)
        
        fields_str = ','.join(field_chunk)
        if data_type == 'PUMS':
            url = f"https://api.census.gov/data/{year}/acs/{acs_type}/pums?get={fields_str}{bg_str}&key={key}"
        else:
            fields_str = 'NAME,' + fields_str
            url = f'https://api.census.gov/data/{year}/acs/{acs_type}?get={fields_str}{bg_str}&key={key}'
            
        # Fetch data from Census API
        chunk = get_with_progress(url)

        chunkcols = chunk[0]
        assert isinstance(chunkcols, list), 'Census API returned an error. Check your API key.'
        
        chunk_df = pd.DataFrame(chunk[1:], columns=chunkcols)
        
        # Join chunks together
        if df is not None:
            assert len(set(df.columns).intersection(chunk_df.columns)) > 0, f'No columns in common between {df.columns} and {chunk_df.columns} to join chunks on'
        # pd.concat([df, chunk_df], axis=1)       )
        df = chunk_df if df is None else pd.merge(df, chunk_df)
    
    # Sort columns
    assert isinstance(df, pd.DataFrame), 'Census API returned an error. Check your API key.'
    col_order = list(set(df.columns).difference(fields)) + fields
    df = df[col_order]
    
    # Confirm that the data types are correct
    dftypes = {k: field_dtypes[k] for k in df.columns if k in field_dtypes}
    df = df.astype(dftypes)
            
    return df

def pqio(data_type: str, state_obj_ls: str, geo_fields: dict, base_path: str, data_dict: dict = {}) -> dict:
    """
    Fetches data from the Census API and writes it to a parquet file.

    Args:
        data_type (str): The data type to fetch. Must be either "PUMS" or "ACS".
        state_obj_ls (str): The list of state objects to fetch data for.
        field_dict (dict): A dictionary of fields and their data type to fetch from the Census API.
        geo_levels (dict): A dictionary of geo levels to fetch data for.
        path (str): Data path to write parquet file to.
        data (pd.DataFrame | None, optional): Existing data to append to. Defaults to None.

    Returns:
        pd.DataFrame: The data fetched from the Census API.
    """

    assert isinstance(data_dict, dict), 'data must be a dictionary'
    
    for geo, fields in geo_fields.items():
        batch_size = 5
        geo_str = geo
        
        # If its PUMS data we set geo string to PUMA for the API call but the geo field is HH or PER
        if data_type == 'PUMS':
            batch_size = 1
            geo_str = 'PUMA'
            
        # Create a backup file before opening to prevent data loss
        for i, state_batch in enumerate(batched(state_obj_ls, batch_size)):
            # Check if data already exists and get the difference
            fips = [getattr(state_obj, 'fips') for state_obj in state_batch]
            ex_fips = getattr(data_dict.get(geo, pd.DataFrame()), 'state', [])
            fips = list(set(fips).difference(ex_fips))
            state_name = [getattr(state_obj, 'name') for state_obj in state_batch]
            
            if len(fips) > 0:
                print(f'\nDownloading {geo} {data_type} data for {state_name}, {i * batch_size} of {len(state_obj_ls)}')                
                    
                # Fetch data from Census API            
                df = api_get(data_type, fips, geo_str, fields)
                
                # Append to the existing data
                if data_dict.get(geo) is None:
                    data_dict[geo] = df
                else:
                    # Check if there is an index to match
                    if data_dict[geo].index.name:
                        df.set_index(data_dict[geo].index.name, inplace=True)                    
                    data_dict[geo] = pd.concat([data_dict[geo], df], axis=0)
        
                # Save updated data
                path = os.path.join(settings.RAW_DATA_DIR, f'{base_path}_{geo}.parquet')
                data_dict[geo].to_parquet(path)
                
            else:
                print(f'\nLoading existing {geo} {data_type} data for {state_name}')
                
    return data_dict

def fetch_from_api(data_type: str) -> dict:
    
    """
    Specify PUMS or ACS to fetch Census data from the Census API and returns a pandas DataFrame.
    
    Args:
        data_type (str): Type of data to fetch. Must be either "PUMS" or "ACS".
        
    Returns:
        pd.DataFrame: Pandas DataFrame of data.
    """
    
    data_type = data_type.upper()
    
    assert data_type in ['PUMS', 'ACS'], 'data_type must be either "PUMS" or "ACS"'
    assert isinstance(settings.STATES, list), 'settings.STATES must be a list'
    assert isinstance(settings.FIPS, list), 'settings.FIPS must be a list'
    
    try:
        data_dict = globals()[data_type]
    except:
        data_dict = {}
        
    fips_list = settings.FIPS
    
    if data_type == 'PUMS':
        # geo_fields = {'PUMA': dict(ele for sub in settings.PUMS_FIELDS.values() for ele in sub.items())}
        geo_fields = settings.PUMS_FIELDS
        base_path = settings.PUMS_DATA_PREFIX                
    else:
        geo_fields = settings.ACS_GEO_FIELDS
        base_path = settings.ACS_DATA_PREFIX
    
    # First check if is already loaded
    try:
        has_states, has_columns = [], []                
        for geo, fields in geo_fields.items():
            state_fips = data_dict[geo].state.apply(lambda x: x.zfill(2))
            _has_states = len(set(fips_list).difference(state_fips)) == 0
            _has_columns = len(set(fields.keys()).difference(data_dict[geo].columns)) == 0
                
            has_states.append(_has_states)
            has_columns.append(_has_columns)
        
        if all(has_states + has_columns):
            return data_dict
    except:
        # Then try to load from parquet and check again
        data_dict = {}
        # Pass labeled kwargs to avoid misordering
        kwargs = {
            'data_type': data_type,
            'state_obj_ls': set(),
            'geo_fields': {},
            'base_path': base_path,
            'data_dict': data_dict
        }
        
        # Check if all states and columns are present, otherwise update geo field to kwargs to be fetched
        for geo, fields in geo_fields.items():
            fpath = f'{settings.RAW_DATA_DIR}/{base_path}_{geo}.parquet'
            
            try:                           
                data_dict[geo] = pd.read_parquet(fpath)
                state_fips = data_dict[geo].state.apply(lambda x: x.zfill(2))
                missing_states = set(fips_list).difference(state_fips)                
                missing_columns = set(fields.keys()).difference(data_dict[geo].columns)
                                
                if len(missing_states) > 0:                    
                    missing_states = set(us.states.lookup(x) for x in missing_states)
                    kwargs['state_obj_ls'] = missing_states.union(kwargs['state_obj_ls'])
                    
                if len(missing_columns) > 0:                    
                    missing_field_states = set(us.states.lookup(x) for x in state_fips)                    
                    kwargs['geo_fields'][geo] = fields
                    kwargs['state_obj_ls'] = missing_field_states.union(kwargs['state_obj_ls'])
                    
                else:
                    print(f'Loading existing {geo} {data_type} data')
                    
            except:
                # Couldn't read the file, so add geo field to kwargs to be updated
                print(f'Could not load existing {geo} {data_type} data, fetching')
                kwargs['state_obj_ls'] = set(us.states.lookup(x) for x in fips_list)
                kwargs['geo_fields'][geo] = fields
                
            # Fetch data, appending the parquet data file for each state.
            if len(kwargs['state_obj_ls']) > 0:
                print(f'Missing data {geo} {data_type} for {len(kwargs["state_obj_ls"])} states')
        data_dict = pqio(**kwargs)
        
    return data_dict

def fetch_pums_from_ftp(year: int = settings.YEAR) -> dict:
    """
    Fetches geography files from the Census FTP server.

    Args:
        geo (str): The geography to fetch. Must be one of 'BG', 'TRACT', 'COUNTY', 'STATE', or 'PUMA'.
        year (int): The year to fetch data for.

    Returns:
        gpd.GeoDataFrame: The geography data.
    """    
    
    geo_fields = settings.PUMS_FIELDS
    base_path = settings.PUMS_DATA_PREFIX  
        
    url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/'
    zips = parse_census_ftp(url, cache_dir=os.path.join(settings.RAW_DATA_DIR, 'csv'), data_type='PUMS')
    
    assert isinstance(zips, list), f'Expected list, got {type(zips)}'
    
    if not os.path.exists(os.path.join(settings.RAW_DATA_DIR, 'csv')):
        os.mkdir(os.path.join(settings.RAW_DATA_DIR, 'csv'))
    
    data_dict = {}
    for geo, fields in geo_fields.items():
        
        # Parquet file path
        pq_path = f'{settings.RAW_DATA_DIR}/{base_path}_{geo}.parquet'
        if os.path.exists(pq_path):
            print(f'Loading existing {geo} PUMS data')
            df = pd.read_parquet(pq_path)
        else:
            df = pd.DataFrame()
        
        # Initialize with existing data
        new_data = [df] 
        
        # Check which state column is present
        state_list = [] 
        if 'ST' in df.columns:
            state_list = df['ST'].unique().astype(str)
            state_list = list(np.char.zfill(state_list, 2))               
        
        # The selected fields to load
        usecols = list(fields.keys())
               
        # Keep only the states we need to add to the parquet file
        level_zips = []
        missing_cols = set(fields.keys()).difference(df.columns)
        is_missing_cols = len(missing_cols) > 0
        
        if is_missing_cols:
            print(f'Missing PUMS columns {missing_cols}, recreating {geo} PUMS cache data')
            new_data = []
            
        for fips_code, fpath, zurl, level in zips:
            is_file = 'csv' in zurl and geo == level.upper()
            is_missing_state = fips_code in settings.FIPS and fips_code not in state_list            
            if is_file and (is_missing_state or is_missing_cols):
                    level_zips.append((fips_code, fpath, zurl, level))
        
        # Loop through the states and download/load the data into dataframes 
        for i, (fips_code, fpath, zurl, level) in enumerate(level_zips, start = 1):        
            # First check if we need to fetch this state
            state_obj = us.states.lookup(fips_code)
            state_name = getattr(state_obj, 'name')
            
            if not os.path.exists(fpath):                    
                print(f'Downloading {state_name} {geo} PUMS data {i} of {len(level_zips)}')                
                bytes_data = get_with_progress(zurl)
                assert isinstance(bytes_data, bytes), f'Expected bytes, got {type(bytes_data)}'
                
                with open(fpath, 'wb') as f:
                    f.write(bytes_data)
                
                # Pass the bytes data to pandas
                fpath = BytesIO(bytes_data)
            else:
                print(f'Loading cached {state_name} {geo} PUMS data {i} of {len(level_zips)}')
            
            with zipfile.ZipFile(fpath, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.csv'):
                        assert isinstance(usecols, list), f'Expected list, got {type(usecols)}'                            
                        df = pd.read_csv(zip_ref.open(file), usecols=usecols, low_memory=False)
                if df is None:    
                    raise ValueError(f'Expected .csv file, got {zip_ref.namelist()}')            
                        
            # Fill NA values with 995 and enforce data types
            df = df.fillna(995).astype(fields)
            
            new_data.append(df)
                    
        if len(new_data) > 1:
            df = pd.concat(new_data, axis=0)
            df.to_parquet(path=pq_path)        

        data_dict[geo] = df
    
    return data_dict

def fetch(data_type: str = 'ACS') -> dict:
    """
    Fetches data from the Census FTP server.

    Args:
        data_type (str): The data type to fetch. Must be one of 'ACS' or 'PUMS'.
        year (int): The year to fetch data for.

    Returns:
        pd.DataFrame: The data.
    """
    
    pums_source = settings.PUMS_SOURCE
    
    assert data_type.upper() in ['ACS', 'PUMS'], f'Expected data_type to be one of "ACS" or "PUMS", got {data_type}'
    assert pums_source.lower() in ['ftp', 'api'], f'Expected pum_source to be one of "ftp" or "api", got {pums_source}'
    
    if data_type == 'PUMS':
        if pums_source == 'ftp':
            data = fetch_pums_from_ftp(settings.YEAR)
        else:    
            data = fetch_from_api(data_type)
    else:
        data = fetch_from_api(data_type)
    
    return data


if __name__ == '__main__':
    # Fetch the data and attach to the module    
    PUMS_DATA = fetch('PUMS')
    ACS_DATA = fetch('ACS')