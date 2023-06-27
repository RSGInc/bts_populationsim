import os
from dotenv import load_dotenv
import pandas as pd
from us import states
import utils

# Load .env file
load_dotenv()

# User-defined constants
YEAR = 2019
STATES = ['VT','AK','ND', 'SD','WY', 'RI'] #[x.abbr for x in states.STATES]
ACS_TYPE = 'acs5'
RAW_DATA_DIR = 'setup/raw'
POPSIM_DIR = 'populationsim'
ACS_DATA_PREFIX = 'acs_data'
PUMS_DATA_PREFIX = 'pums_data'


# Inferred constants
CENSUS_API_KEY = os.getenv('CENSUS_API_KEY')
STATES = STATES if isinstance(STATES, list) else [STATES]
FIPS = fips_list = [getattr(states.lookup(x), 'fips') for x in STATES] 


ACS_AGGREGATOR = pd.read_csv(os.path.join(POPSIM_DIR, 'configs/target_aggregator.csv'))
PUMS_AGGREGATOR = pd.read_csv(os.path.join(POPSIM_DIR, 'configs/controls.csv'))

# Targets must match!
# assert set(ACS_AGGREGATOR.target) == set(PUMS_AGGREGATOR.target), f'ACS and PUMS targets do not match!'

ACS_AGGREGATION, ACS_GEO_FIELDS, ACS_TABLES, CONTROL_FIELDS = utils.aggregate_acs_fields(ACS_AGGREGATOR)
PUMS_FIELDS = {
    'HH': {
        'SERIALNO': str,
        'PUMA': int, 
        'WGTP':int, 
        'NP': int,
        'HINCP': int, 
        'VEH': int,
        'HUPAC': int,
    },
    'PER': {
        'SPORDER': int,
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

# CONSTANTS
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