import pandas as pd
import re

# Functions
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
    
    return (field_agg, geo_fields, control_fields, tables)

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