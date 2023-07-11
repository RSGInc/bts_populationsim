# bts_populationsim


## Setup environment

I created a environment setup batch script to create a conda environment. This will essentiall run the following steps:

You can use whatever preferred Python environment management system, but I will demonstrate using Mamba, a faster conda alternative.

1. After cloning this repository, clone the `populationsim` and `activitysim` source code into the `./src` folder. The `./src` folder is ignored by git, so you can put anything you want in there. I like to keep all my source code in one place:
<br>
```git clone https://github.com/ActivitySim/populationsim.git ./src/populationsim```
<br><br>
I found that the latest version of activitysim had a sharrow-related bug that affects multiprocessing, so you may want clone an older release:
<br>
```git clone -b v1.2.0 https://github.com/ActivitySim/activitysim.git@v1.2.1 ./src/activitysim```

2. Create the mamba environment from the environment yaml recipe:
```mamba env create --file bts_env.yml```
<br><br>
This will create an environment and install an editable version of populationsim and activitysim as well as any supporting packages. Editable means that any changes in the `./src/populationsim` folder are reflected in the environment. This makes debugging easier and allows you to make changes to the source code without having to reinstall the package.

3. Activate the environment:
```conda activate bts_populationsim```

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
# STATES = ['VT','AK','ND', 'SD','WY', 'RI']
STATES = [x.abbr for x in states.STATES]
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

The .env file is ignored by git, keeping your API key private.


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

## Running

To run the setup scripts you can either manually run the `prepare_data.py` script, or use the CLI by executing `python -m setup` from the root of the repository.

The setup scripts have three main components:
- create_acs_targets(): This function fetches the ACS data from the Census API and caches it into local parquet files. It then aggregates the fields and saves the aggregated data to control_totals CSV files in the `populationsim/data` folder.
- create_seeds(): This function fetches the PUMS data from Census API and caches it into local parquet files. It then formats the fields and saves the seed data to seed_household and seed_person CSV files in the `populationsim/data` folder.
- create_crosswalk(): This function fetches the relevant geography files (e.g., block groups, tracts, PUMAs, etc.), saves them locally in the `setup/raw/shp` folder, and creates a crosswalk between the PUMS and ACS geographies. The crosswalk is saved to the `populationsim/data` folder.

