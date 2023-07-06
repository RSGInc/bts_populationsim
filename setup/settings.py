import os
from dotenv import load_dotenv
import pandas as pd
from us import states
import settings_helper

# Load .env file
load_dotenv()

# User-defined constants
YEAR = 2021
POPSIM_DIR = 'C:/gitclones/bts_populationsim/populationsim'
SETUP_DIR = 'C:/gitclones/bts_populationsim/setup'
RAW_DATA_DIR = os.path.join(SETUP_DIR, 'raw')

# STATES = ['VT','AK','ND', 'SD','WY', 'RI', 'MT', 'UT']
# STATES = ['CA', 'NY', 'TX', 'WA']
STATES = [x.abbr for x in states.STATES]
ACS_TYPE = 'acs5'

"""
You must define the PUMS fields you want to use for households and persons,
grouped in a nested dictionary by table. The fields must also specify the data
type (int, float, str, etc.) to ensure that the data is read in correctly.
"""
PUMS_FIELDS = {
    'HH': {
        'SERIALNO': str,
        'PUMA': int,
        'ST': int,
        'WGTP':int, 
        'NP': int,
        'HINCP': int, 
        'VEH': int,
        'HUPAC': int,
    },
    'PER': {
        'SERIALNO': str,
        'SPORDER': int,
        'PUMA': int,
        'ST': int,
        'PWGTP': int,
        'JWTRNS': int,        
        'ESR': int,        
        'SCH': int,
        'SCHG': int,
        'AGEP': int,
        'SEX': int,
        'RAC1P': int,
        'HISP': int,
        'WKHP': int,
    }
}

RENAME = {
    'ST': 'STATE',
    'BLOCK GROUP': 'BG',
}

# Inferred constants
CENSUS_API_KEY = os.getenv('CENSUS_API_KEY')
STATES = STATES if isinstance(STATES, list) else [STATES]
FIPS = [getattr(states.lookup(x), 'fips') for x in STATES] 

ACS_AGGREGATOR = pd.read_csv(os.path.join(SETUP_DIR, 'controls_aggregator.csv'))
PUMS_AGGREGATOR = pd.read_csv(os.path.join(POPSIM_DIR, 'configs/controls.csv'))

# Extract fields from aggregators
ACS_AGGREGATION, ACS_GEO_FIELDS, CONTROL_FIELDS, ACS_TABLES = settings_helper.aggregate_acs_fields(ACS_AGGREGATOR)

# Control fields must match!
assert set(CONTROL_FIELDS) == set(PUMS_AGGREGATOR.control_field),\
    f'Missing field {set(CONTROL_FIELDS).difference(PUMS_AGGREGATOR.control_field)},\
        ACS and PUMS control fields do not match!'

# CONSTANTS
ACS_DATA_PREFIX = 'acs_data'
PUMS_DATA_PREFIX = 'pums_data'
PUMS_SOURCE = 'ftp'

GEOID_LEN = {
    'STATE': 2,
    'COUNTY': 3,
    'TRACT': 6,
    'BG': 1,
    'PUMA': 3
    }

GEOID_STRUCTURE = {
    'BG': ['STATE', 'COUNTY', 'TRACT', 'BG'],
    'TRACT': ['STATE', 'COUNTY', 'TRACT'],
    'COUNTY': ['STATE', 'COUNTY'],
    'STATE': ['STATE'],
    'PUMA': ['STATE', 'PUMA']
}