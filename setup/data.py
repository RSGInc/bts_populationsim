import settings
import us
import os
import pandas as pd
from utils import get_with_progress, batched
import pyarrow as pa
import pyarrow.parquet as pq


def api_get(data_type: str, state_fips: int|str|list, geo:str, field_dtypes: dict) -> pd.DataFrame:
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
    chunk_size = 40 if data_type == 'ACS' else 5
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
        print(f'Fetching fields {i} to {i + chunk_size} of {len(fields)}')        
        field_chunk = fields[i:i + chunk_size]
        # Assure that SERIALNO and SPORDER are included in PUMS data requests
        if data_type == 'PUMS':
            for x in ['SERIALNO', 'SPORDER']:
                if x not in field_chunk:
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
        if df:
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
        pqwriter = None
        
        batch_size = 5 if data_type == 'ACS' else 1
        
        for i, state_batch in enumerate(batched(state_obj_ls, batch_size)):
            # Check if data already exists and get the difference
            fips = [getattr(state_obj, 'fips') for state_obj in state_batch]
            ex_fips = getattr(data_dict.get(geo, pd.DataFrame()), 'state', [])
            fips = list(set(fips).difference(ex_fips))
            state_name = [getattr(state_obj, 'name') for state_obj in state_batch]
            
            if len(fips) > 0:
                print(f'\nDownloading {geo} {data_type} data for {state_name}, {i * batch_size} of {len(state_obj_ls)}')                
                
                # Fetch data from Census API            
                df = api_get(data_type, fips, geo, fields)
                
                # Convert to pyarrow table
                table = pa.Table.from_pandas(df)
                
                # Intialize parquet writer if empty            
                if pqwriter is None:                    
                    path = os.path.join(settings.RAW_DATA_DIR, f'{base_path}_{geo}.parquet')
                    pqwriter = pq.ParquetWriter(path, table.schema)
                
                # Write table to parquet and append to data
                pqwriter.write_table(table)
                if data_dict.get(geo) is None:
                    data_dict[geo] = df
                else:
                    data_dict[geo] = df
                
            else:
                print(f'\nLoading existing {data_type} data for {state_name}')
                
        if pqwriter:            
            pqwriter.close()
                    
    return data_dict

def fetch(data_type: str) -> dict:
    
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
        field_dict = dict(ele for sub in settings.PUMS_FIELDS.values() for ele in sub.items())
        geo_fields = {'PUMA': field_dict}
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
                
                if len(missing_states) > 0 or len(missing_columns) > 0:                    
                    missing_states = set(us.states.lookup(x) for x in missing_states)                    
                    kwargs['geo_fields'][geo] = fields
                    kwargs['state_obj_ls'] = missing_states.union(kwargs['state_obj_ls'])
                    
                else:
                    print(f'Loading existing {geo} {data_type} data')
                    
            except:
                # Couldn't read the file, so add geo field to kwargs to be updated
                print(f'Could not load existing {geo} {data_type} data, fetching')
                kwargs['state_obj_ls'] = set(us.states.lookup(x) for x in fips_list)
                kwargs['geo_fields'][geo] = fields
                
        # Fetch data, appending the parquet data file for each state.
        print(f'Missing data for {len(kwargs["state_obj_ls"])} states')
        data_dict = pqio(**kwargs)
        
    return data_dict


# Fetch the data and attach to the module
ACS_DATA = fetch('ACS')
PUMS_DATA = fetch('PUMS')