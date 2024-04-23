# bts_populationsim


## Setup environment

1. **Create a new conda environment:** You can install and run PopulationSim in default “base” python environment or you can set up new environment:
   conda create --name bts_popS python=3.11.5 
Use the following command to activate the environment:
    conda activate bts_popS 
2. **Install all dependencies and PopulationSim:**
After cloning this repository, install all dependencies and the forked version of PopulationSim either directly from GitHub or with Conda/Mamba:

2a. **Installing from GitHub:** Install populationsim fork directly from GitHub using pip. This will install all dependencies and the forked version of PopulationSim to your current Python environment.

pip install git+https://github.com/nick-fournier-rsg/populationsim.git@v0.6.1#egg=populationsim
<br><br>

2b. **Installing with Conda/Mamba:** The easiest way to install the fork is to use Conda or Mamba. This will install all dependencies and the forked version of PopulationSim.
<br><br>
Detail installation direction can be found in this link:
<br>
https://github.com/nick-fournier-rsg/populationsim

3. **Install required python packages:** Intall all the required python packages in the environment to run populatiosim using following command:
    pip install -r requirements.txt 

## Configuration
I created a set of data preparation scripts which fetch Census PUMS and ACS data and prepare it for use with populationsim. The scripts are located in the `./setup` folder. 

### settings.py
To configure the setup scripts (e.g., selecting States, ACS years, fields), you can edit the `settings.py` file. The settings file is a Python module that gets inherited by the setup scripts. An example settings file is shown below:

```python
# User-defined constants
YEAR = 2019
POPSIM_DIR = 'C:/gitclones/bts_populationsim/populationsim'
SETUP_DIR = 'C:/gitclones/bts_populationsim/setup'
RAW_DATA_DIR = os.path.join(SETUP_DIR, 'raw')

# Specific states, or just list all states in the 'states' module!
# STATES = ['VT','AK','ND', 'SD','WY', 'RI'] # Explicitly list specific states
STATES = [x.abbr for x in states.STATES] # lists all states in the states module
ACS_TYPE = 'acs5'

# You must define the PUMS fields you want to use for households and persons, grouped in a nested dictionary by table. The fields must also specify the data type (int, float, str, etc.) to ensure that the data is read in correctly.
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
```

### Census API Key
The setup scripts require a Census API key. You can get one here: https://api.census.gov/data/key_signup.html, once you have one, you can set it as an environment variable in a `.env` file in the root of the repository that is inherited by the setup scripts The `.env` file should look like this:
```CENSUS_API_KEY="YOUR_API_KEY"```

The .env file is ignored by git, keeping your API key private so you will need to create one for yourself in your local repository. It then is loaded into the python environment in the `settings.py` file using the [`python-dotenv`](https://pypi.org/project/python-dotenv/) package with `load_dotenv()`.


### controls_aggregator.csv
In addition to the populationsim-specific configs in the `configs` folder, there is a `controls_aggregator.csv` file that is used to aggregate the PUMS data to the control totals. The `controls_aggregator.csv` file is a CSV file with the following columns:

- `field`: [mandatory] The field code in the Census (e.g., `B01001_003E`)
- `type`: [mandatory] The data type of field (e.g., `int` or `str`)
- `geography`: [mandatory] the geography that the control total available at (e.g., `BG` or `TRACT`)
- `control_field`: [user defined] the control field to aggregate to (e.g., `H_RACE_AAPI` or `H_INC_1`)
- `original_label`: [optional] The original Census label of the field.
- `concept`: [optional] The Census concept of the field [e.g., `SEX BY AGE`]
- `group`: [optional] The table that the field belongs to (e.g., `B01001`)

You may populate this file from the `raw/acs_fields.csv` and `raw/acs_tables.csv`, which just has a list of tables and the fields.

### Data Preparation

The setup scripts have three main components:
- create_acs_targets(): This function fetches the ACS data from the Census API and caches it into local parquet files. It then aggregates the fields and saves the aggregated data to control_totals CSV files in the `populationsim/data` folder.
- create_seeds(): This function fetches the PUMS data from Census API and caches it into local parquet files. It then formats the fields and saves the seed data to seed_household and seed_person CSV files in the `populationsim/data` folder.
- create_crosswalk(): This function fetches the relevant geography files (e.g., block groups, tracts, PUMAs, etc.), saves them locally in the `setup/raw/shp` folder, and creates a crosswalk between the PUMS and ACS geographies. The crosswalk is saved to the `populationsim/data` folder.


## Running

The entire process can be run from the `batch_run.py` script. This script acts as the single point of entry for the data preparation, populationsim, and validation steps.

It can be run from command line with the following command:
```
python batch_run.py
```