import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from us import states
import yaml

from setup_inputs.settings_helpers import aggregate_acs_fields


# Load .env file
load_dotenv()

# User-defined constants
YEAR = 2021
# POPSIM_DIR = 'C:/gitclones/bts_populationsim/populationsim'
POPSIM_DIR = os.path.join(Path(__file__).parent.parent.absolute(), 'populationsim')
SETUP_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(SETUP_DIR, 'raw')

# ACS data | This just will list all states
STATES = [x.abbr for x in states.STATES]
ACS_TYPE = 'acs5'
BATCH_SIZE = 50

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

# ACS Remainder columns, e.g., p_mode_na = p_total - (p_mode1 + p_mode2 + ...)
ACS_REMAINDERS = {
    'P_MODE_NA': 
        {
            'GEOGRAPHY': 'TRACT',
            'ADD_COLS': 'P_TOTAL',
            'SUBTRACT_COLS': ['P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH']
        },
    'P_NON_UNIVERSITY':
        {
            'GEOGRAPHY': 'BG',
            'ADD_COLS': 'P_TOTAL',
            'SUBTRACT_COLS': ['P_UNIVERSITY']
        },
    'P_NON_WORKER':
        {
            'GEOGRAPHY': 'BG',
            'ADD_COLS': 'P_TOTAL',
            'SUBTRACT_COLS': ['P_FULL_TIME', 'P_PART_TIME']
        },  
}

# -------------------_DO NOT EDIT BELOW THIS LINE_------------------- #
# Inferred constants
CENSUS_API_KEY = os.getenv('CENSUS_API_KEY')
STATES = STATES if isinstance(STATES, list) else [STATES]
FIPS = [getattr(states.lookup(x), 'fips') for x in STATES] 

ACS_AGGREGATOR = pd.read_csv(os.path.join(POPSIM_DIR, 'configs/controls_aggregator.csv'))
PUMS_AGGREGATOR = pd.read_csv(os.path.join(POPSIM_DIR, 'configs/controls.csv'))

# Extract fields from aggregators
ACS_AGGREGATION, ACS_GEO_FIELDS, CONTROL_FIELDS, ACS_TABLES = aggregate_acs_fields(ACS_AGGREGATOR)
CONTROL_FIELDS.extend(ACS_REMAINDERS.keys())

# Control fields must match!
assert set(CONTROL_FIELDS) == set(PUMS_AGGREGATOR.control_field.to_list()),\
    f'Missing field {set(CONTROL_FIELDS) ^ set(PUMS_AGGREGATOR.control_field)}, ACS and PUMS control fields do not match!'

# Other constants
ACS_DATA_PREFIX = 'acs_data'
PUMS_DATA_PREFIX = 'pums_data'
PUMS_SOURCE = 'ftp'
CHECKSUM_TOLERANCE = 0.01 # 1% tolerance for checksums

# Read popsim yaml
with open(os.path.join(POPSIM_DIR, 'configs/settings.yaml')) as f:
    POPSIM_SETTINGS = yaml.load(f, Loader=yaml.FullLoader)    

GEOID_LEN = {
    'STATE': 2,
    'COUNTY': 3,
    'TRACT': 6,
    'BG': 1,
    'PUMA': 5
    }

GEOID_STRUCTURE = {
    'BG': ['STATE', 'COUNTY', 'TRACT', 'BG'],
    'TRACT': ['STATE', 'COUNTY', 'TRACT'],
    'COUNTY': ['STATE', 'COUNTY'],
    'STATE': ['STATE'],
    'PUMA': ['STATE', 'PUMA']
}