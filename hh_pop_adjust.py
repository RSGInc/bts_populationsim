import os
import requests
import json
import pandas as pd
import numpy as np
from census import Census
import us
from us import states
os.environ['DC_STATEHOOD'] = '1'
from dotenv import load_dotenv

## configure from JSON file
with open('hh_config.json', 'r') as file:
    config = json.load(file)

# Load .env file
load_dotenv()
api_key = os.getenv('CENSUS_API_KEY')

state_fips = '11' ## change the fips here

def fetch_pums_data(year, dataset, state_fips, variables, api_key):
    url = f'https://api.census.gov/data/{year}/acs/{dataset}/pums?get={variables},ST,PUMA&for=public%20use%20microdata%20area:*&in=state:{state_fips}&key={api_key}'
    response = requests.get(url)
    
    if response.status_code == 200:
        census_data = response.json()
        df = pd.DataFrame(census_data[1:], columns=census_data[0])
        df['ST'] = df['ST'].apply(lambda x: f'{int(x):02}')
        df['PUMA'] = df['PUMA'].apply(lambda x: f'{int(x):05}')
        df['PUMA'] = df['ST'] + df['PUMA']
        return df
    else:
        print(f'Error fetching data: {response.status_code}')
        print(response.text)  
        return None


# ==========================================
## Households: WGTP
# ==========================================

### for 1-year data 
df_1yr_wgtp = fetch_pums_data(year='2019', dataset='acs1', state_fips=state_fips, variables='WGTP', api_key=api_key)
df_1yr_wgtp = df_1yr_wgtp.apply(pd.to_numeric, errors = 'ignore')
df_1yr_wgtp.drop(columns=["state", "public use microdata area"], inplace=True)

## summation of 1-year WGTP for each PUMA
sum_1yr_wgtp_puma = df_1yr_wgtp.groupby('PUMA')['WGTP'].sum().reset_index()
sum_1yr_wgtp_puma.rename(columns={'WGTP':'PUMA_SUM_1yr'}, inplace=True)

## summation of 1-year WGTP for state
sum_1yr_wgtp_state = df_1yr_wgtp['WGTP'].sum()

### for 5year data
df_5yr_wgtp = fetch_pums_data(year='2021', dataset='acs5', state_fips=state_fips, variables='WGTP', api_key=api_key)
df_5yr_wgtp = df_5yr_wgtp.apply(pd.to_numeric, errors = 'ignore')
df_5yr_wgtp.drop(columns=["state", "public use microdata area"], inplace=True)

# summation of 5-year WGTP for each PUMA
sum_5yr_wgtp_puma = df_5yr_wgtp.groupby('PUMA')['WGTP'].sum().reset_index()
sum_5yr_wgtp_puma.rename(columns={'WGTP':'PUMA_SUM_5yr'},inplace=True)

## summation of 5-year WGTP for state
sum_5yr_wgtp_state = df_5yr_wgtp['WGTP'].sum()


# estimate adjustment factor for each PUMA and State
wgtp_1yr_5yr_puma = pd.merge(sum_5yr_wgtp_puma, sum_1yr_wgtp_puma, on='PUMA', how='left')                   # merge 1-year and 5-year data for PUMA level
wgtp_1yr_5yr_puma['ADJ_FACTOR_HH'] = wgtp_1yr_5yr_puma['PUMA_SUM_1yr'] / wgtp_1yr_5yr_puma['PUMA_SUM_5yr']     # estimate the adjustment factor for each PUMA
wgtp_puma_adj_factor = wgtp_1yr_5yr_puma[['PUMA', 'ADJ_FACTOR_HH']]                                           

wgtp_state_adj_factor = sum_1yr_wgtp_state/sum_5yr_wgtp_state


# ==========================================
## Persons: PWGTP
# ==========================================
df_1yr_pwgtp = fetch_pums_data(year='2019', dataset='acs1', state_fips=state_fips, variables='PWGTP', api_key=api_key)
df_1yr_pwgtp = df_1yr_pwgtp.apply(pd.to_numeric, errors = 'ignore')
df_1yr_pwgtp.drop(columns=["state", "public use microdata area"], inplace=True)


sum_1yr_wgtp_puma = df_1yr_pwgtp.groupby('PUMA')['PWGTP'].sum().reset_index()
sum_1yr_wgtp_puma.rename(columns={'PWGTP':'PUMA_SUM_1yr'}, inplace= True)

sum_1yr_wgtp_state = df_1yr_pwgtp['PWGTP'].sum()

df_5yr_pwgtp = fetch_pums_data(year='2021', dataset='acs5', state_fips=state_fips, variables='PWGTP', api_key=api_key)
df_5yr_pwgtp = df_5yr_pwgtp.apply(pd.to_numeric, errors = 'ignore')
df_5yr_pwgtp.drop(columns=["state", "public use microdata area"], inplace=True)


sum_5yr_pwgtp_puma = df_5yr_pwgtp.groupby('PUMA')['PWGTP'].sum().reset_index()
sum_5yr_pwgtp_puma.rename(columns={'PWGTP':'PUMA_SUM_5yr'},inplace=True)

sum_5yr_pwgtp_state = df_5yr_pwgtp['PWGTP'].sum()

#merge 1-year and 5-year data
pwgtp_1yr_5yr_puma = pd.merge(sum_5yr_pwgtp_puma, sum_1yr_wgtp_puma, on='PUMA', how='left')
pwgtp_1yr_5yr_puma['ADJ_FACTOR_PER'] = pwgtp_1yr_5yr_puma['PUMA_SUM_1yr'] / pwgtp_1yr_5yr_puma['PUMA_SUM_5yr']
pwgtp_puma_adj_factor = pwgtp_1yr_5yr_puma[['PUMA', 'ADJ_FACTOR_PER']]

pwgtp_state_adj_factor = sum_1yr_wgtp_state/sum_5yr_pwgtp_state

# Import data
# create main folder path
system_path = os.getcwd()
system_path_populationsim = os.path.join(system_path, 'populationsim')
main_folder_path = os.path.join(system_path_populationsim, 'data')

def list_files_for_states(state_path):
        """
    Lists and reads all CSV files in the specified state folder.

    Args:
    state_folder (str): The name of the state folder to list CSV files from.

    Returns:
    list: A dictionary with file names as keys and DataFrames as value.
    """
        state_folder_path = os.path.join(main_folder_path, state_path)
        if not os.path.exists(state_folder_path):
            raise FileNotFoundError(f"The folder {state_path} does not exist in the main directory.")
        
        state_data_files = [file for file in os.listdir(state_folder_path) if file.endswith('.csv')] 

        dataframes = {}
        for csv_file in state_data_files:
            file_path = os.path.join(state_folder_path, csv_file)
            df = pd.read_csv(file_path)
            dataframes[csv_file] = df

        return dataframes

state_abr = config['state_abr']
# STATE = 'CT' # change state directory here
STATE = state_abr.get(state_fips, 'Unknown')
state_data_files = list_files_for_states(STATE)

for file_name, df in state_data_files.items():
#     print(f"File: {file_name}")
    
    if file_name == 'seed_households.csv':
         seed_hh = df
    if file_name == 'seed_persons.csv':
         seed_per = df
    if file_name == 'control_totals_BG.csv':
         control_tot_BG = df
    if file_name == 'control_totals_STATE.csv':
         control_tot_STATE = df
    if file_name == 'control_totals_TRACT.csv':
         control_tot_TRACT = df
    if file_name == 'scaled_control_totals_meta.csv':
         scaled_control = df
    if file_name == 'geo_cross_walk.csv':
         geo_crosswalk = df

# re-order of the columns
variables_order = [
                    'COUNTY','BG','TRACT','STATE', 'NAME', 'REGION',
                    'H_TOTAL', 
                    'H_CHILDREN', 'H_NO_CHILDREN', 
                    'H_INCOME_0_25', 'H_INCOME_25_50', 'H_INCOME_50_75', 'H_INCOME_75_100', 'H_INCOME_100_150', 'H_INCOME_150PLUS', 
                    'H_SIZE_1', 'H_SIZE_2', 'H_SIZE_3', 'H_SIZE_4', 'H_SIZE_5PLUS', 
                    'H_NO_VEH', 'H_VEH_1', 'H_VEH_2', 'H_VEH_3', 'H_VEH_4MORE', 
                    'H_OWNER', 'H_RENTER',
                    'H_MORTGAGE_0_799', 'H_MORTGAGE_800_1499', 'H_MORTGAGE_1500_2499', 'H_MORTGAGE_2500PLUS', 'H_NO_MORTGAGE_0_799', 'H_NO_MORTGAGE_800_1499', 'H_NO_MORTGAGE_1500PLUS', 
                    'H_RENT_0_799', 'H_RENT_800_1249', 'H_RENT_1250_1999', 'H_RENT_2000PLUS', 
                    'P_TOTAL', 
                    'P_MALE', 'P_FEMALE',
                    'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64', 'P_AGE_65PLUS', 
                    'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_OTHER', 'P_RACE_AAPI', 
                    'P_NON_HISPANIC', 'P_HISPANIC', 
                    'P_FULL_TIME', 'P_PART_TIME', 
                    'P_UNIVERSITY',
                    'P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH', 'P_MODE_NA', 'P_NON_WORKER', 'P_NON_UNIVERSITY'
]
variables_state = [col for col in variables_order if col in control_tot_STATE.columns]
variables_scale = [col for col in variables_order if col in scaled_control.columns]
variables_bg = [col for col in variables_order if col in control_tot_BG.columns]
variables_tract = [col for col in variables_order if col in control_tot_TRACT.columns]

if variables_state:
    control_tot_STATE = control_tot_STATE[variables_state]
if variables_scale:
    scaled_control = scaled_control[variables_scale]
if variables_bg:
    control_tot_BG = control_tot_BG[variables_bg]
if variables_tract:
    control_tot_TRACT = control_tot_TRACT[variables_tract]

##multiply with seed_households and seed_person
seed_hh = pd.merge(seed_hh, wgtp_puma_adj_factor, on='PUMA', how='left')
seed_hh['WGTP'] = seed_hh['WGTP'] * seed_hh['ADJ_FACTOR_HH']
seed_hh.drop(columns=['ADJ_FACTOR_HH'], inplace=True)

seed_per = pd.merge(seed_per, pwgtp_puma_adj_factor, on='PUMA', how='left')
seed_per['PWGTP'] = seed_per['PWGTP'] * seed_per['ADJ_FACTOR_PER']
seed_per.drop(columns=['ADJ_FACTOR_PER'], inplace=True)

BG_ID = geo_crosswalk[['BG', 'PUMA']]
TRACT_ID = geo_crosswalk[['TRACT', 'PUMA']]
TRACT_ID = TRACT_ID.drop_duplicates(subset='TRACT', ignore_index= True)

adjustment_factor_BG = pd.merge(BG_ID, wgtp_puma_adj_factor, on= 'PUMA', how= 'left')
adjustment_factor_BG = pd.merge(adjustment_factor_BG, pwgtp_puma_adj_factor, on= 'PUMA', how= 'left')
control_tot_BG = control_tot_BG.merge(adjustment_factor_BG, on='BG', how='left')
control_tot_BG = control_tot_BG.fillna(1)

adjustment_factor_TRACT = pd.merge(TRACT_ID, wgtp_puma_adj_factor, on= 'PUMA', how= 'left')
adjustment_factor_TRACT = pd.merge(adjustment_factor_TRACT, pwgtp_puma_adj_factor, on= 'PUMA', how= 'left')
control_tot_TRACT = control_tot_TRACT.merge(adjustment_factor_TRACT, on='TRACT', how='left')
control_tot_TRACT = control_tot_TRACT.fillna(1)

control_tot_TRACT.isna().sum()
control_tot_BG.isna().sum()

st_hh_col = ['H_TOTAL', 
             'H_CHILDREN', 'H_NO_CHILDREN', 
             'H_INCOME_0_25', 'H_INCOME_25_50', 'H_INCOME_50_75', 'H_INCOME_75_100', 'H_INCOME_100_150', 'H_INCOME_150PLUS', 
             'H_SIZE_1', 'H_SIZE_2', 'H_SIZE_3', 'H_SIZE_4', 'H_SIZE_5PLUS',
             'H_NO_VEH', 'H_VEH_1', 'H_VEH_2', 'H_VEH_3', 'H_VEH_4MORE',
             'H_OWNER', 'H_RENTER',
             'H_MORTGAGE_0_799', 'H_MORTGAGE_800_1499', 'H_MORTGAGE_1500_2499', 'H_MORTGAGE_2500PLUS', 'H_NO_MORTGAGE_0_799', 'H_NO_MORTGAGE_800_1499', 'H_NO_MORTGAGE_1500PLUS',
             'H_RENT_0_799', 'H_RENT_800_1249', 'H_RENT_1250_1999', 'H_RENT_2000PLUS']

st_per_col = ['P_TOTAL', 
              'P_MALE', 'P_FEMALE', 
              'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64', 'P_AGE_65PLUS', 
              'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_AAPI', 'P_RACE_OTHER',
              'P_HISPANIC', 'P_NON_HISPANIC',
              'P_FULL_TIME', 'P_PART_TIME',  
              'P_UNIVERSITY',
              'P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH']

tc_per_col = ['P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH', 'P_MODE_NA']

bg_hh_col = st_hh_col
bg_per_col = ['P_TOTAL', 
              'P_MALE', 'P_FEMALE', 
              'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64', 'P_AGE_65PLUS', 
              'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_AAPI', 'P_RACE_OTHER',
              'P_HISPANIC', 'P_NON_HISPANIC',
              'P_FULL_TIME', 'P_PART_TIME', 
              'P_UNIVERSITY','P_NON_WORKER', 'P_NON_UNIVERSITY']

scale_hh_col = st_hh_col
scale_per_col = st_per_col


control_tot_BG[bg_hh_col] = control_tot_BG[bg_hh_col].apply(lambda x: x * control_tot_BG['ADJ_FACTOR_HH'], axis=0)
control_tot_BG[bg_per_col] = control_tot_BG[bg_per_col].apply(lambda x: x * control_tot_BG['ADJ_FACTOR_PER'], axis=0)
control_tot_BG = control_tot_BG.drop(columns=['PUMA', 'ADJ_FACTOR_HH', 'ADJ_FACTOR_PER'])

control_tot_STATE[st_hh_col] = control_tot_STATE[st_hh_col] * wgtp_state_adj_factor
control_tot_STATE[st_per_col] = control_tot_STATE[st_per_col] * pwgtp_state_adj_factor

# control_tot_TRACT[tc_hh_col] = control_tot_TRACT[tc_hh_col].apply(lambda x: x * control_tot_TRACT['ADJ_FACTOR_HH'], axis=0)
control_tot_TRACT[tc_per_col] = control_tot_TRACT[tc_per_col].apply(lambda x: x * control_tot_TRACT['ADJ_FACTOR_PER'], axis=0)
control_tot_TRACT = control_tot_TRACT.drop(columns=['PUMA', 'ADJ_FACTOR_HH', 'ADJ_FACTOR_PER'])

scaled_control[scale_hh_col] = scaled_control[scale_hh_col] * wgtp_state_adj_factor
scaled_control[scale_per_col] = scaled_control[scale_per_col] * pwgtp_state_adj_factor

## fetching variables from acs census data table

variables = config['variables']
estimated_variables = config['estimated_variables']

def fetch_acs_data(state_fips):
     data = Census(api_key).acs1.state(variables, state_fips, year=2019)
     df = pd.DataFrame(data)

     #estimated variables for control_variables
     for control_var, components in estimated_variables.items():
          df[control_var] = sum(df[var].astype(int) for var in components)
     
     # keeping only the control variables
     derived_var = list(estimated_variables.keys())
     df = df[derived_var]
     df['STATE'] = state_fips
     df = df[['STATE'] + derived_var]
     return df

try:
    all_state_acs_df
    data_already_fetched = True
except NameError:
    data_already_fetched = False

if not data_already_fetched:
    all_data = []
    added_states = set()

    for state in us.states.STATES:
        if state.name not in added_states:
            acs_data = fetch_acs_data(state.fips)
            all_data.append(acs_data)
            added_states.add(state.name)

    dc_fips = '11'
    if 'District of Columbia' not in added_states:
        dc_data = fetch_acs_data(dc_fips)
        all_data.append(dc_data)
        added_states.add('District of Columbia')
        
    all_state_acs_df = pd.concat(all_data, ignore_index=True)

# all_data = []
# added_states = set()

# for state in states.STATES:
#      if state.name not in added_states:
#           acs_data = fetch_acs_data(state.fips)
#           # acs_data['state'] = state.name
#           all_data.append(acs_data)
#           added_states.add(state.name)
     
# dc_fips = '11'
# if 'District of Columbia' not in added_states:
#     dc_data = fetch_acs_data(dc_fips)
#     all_data.append(dc_data)
#     added_states.add('District of Columbia')


# all_state_acs_df = pd.concat(all_data, ignore_index=True)
# # all_state_acs_df.to_csv(os.path.join(main_folder_path, 'acs1_2019_control.csv'),index=False)
each_state_df = pd.DataFrame(all_state_acs_df.loc[all_state_acs_df['STATE'] == state_fips]) 
each_state_df= each_state_df.reset_index(drop=True)

## additional adjustment of WGTP and PWGTP
add_wgtp_adj = (each_state_df['H_TOTAL']/(seed_hh['WGTP'].sum())).iloc[0]
seed_hh['WGTP'] = seed_hh['WGTP'] * add_wgtp_adj
add_pwgtp_adj = (each_state_df['P_TOTAL']/(seed_per['PWGTP'].sum())).iloc[0]
seed_per['PWGTP'] = seed_per['PWGTP'] * add_pwgtp_adj

# estimate additional variables
additional_variables_tract_bg = pd.DataFrame(control_tot_BG[['P_NON_WORKER', 'P_NON_UNIVERSITY']].sum()).transpose()
additional_variables_tract_bg['P_MODE_NA'] = pd.DataFrame(control_tot_TRACT[['P_MODE_NA']].sum()).transpose()
additional_variables_acs = pd.DataFrame()
additional_variables_acs['P_NON_WORKER'] = each_state_df['P_TOTAL'] - each_state_df['P_FULL_TIME'] - each_state_df['P_PART_TIME']
additional_variables_acs['P_NON_UNIVERSITY'] = each_state_df['P_TOTAL'] - each_state_df['P_UNIVERSITY']
additional_variables_acs['P_MODE_NA'] = each_state_df['P_TOTAL'] - each_state_df['P_MODE_AUTO_OTHER'] - each_state_df['P_MODE_TRANSIT'] - each_state_df['P_MODE_WALK_BIKE'] - each_state_df['P_MODE_WFH']

each_state_df.loc[:, ~each_state_df.columns.isin(['STATE'])] /= control_tot_STATE.loc[:, ~control_tot_STATE.columns.isin(['STATE'])]
additional_variables_acs /= additional_variables_tract_bg

each_state_df = pd.concat([each_state_df, additional_variables_acs], axis=1)

## adjust final dataset 
control_tot_STATE.loc[:, ~control_tot_STATE.columns.isin(['STATE', 'P_NON_WORKER', 'P_NON_UNIVERSITY', 'P_MODE_NA'])]  *= each_state_df.loc[:, ~each_state_df.columns.isin(['STATE', 'P_NON_WORKER', 'P_NON_UNIVERSITY', 'P_MODE_NA'])]
scaled_control.loc[:, ~scaled_control.columns.isin(['REGION', 'P_NON_WORKER', 'P_NON_UNIVERSITY', 'P_MODE_NA'])]  *= each_state_df.loc[:, ~each_state_df.columns.isin(['STATE', 'P_NON_WORKER', 'P_NON_UNIVERSITY', 'P_MODE_NA'])]


## match the shape of the adjustment factor dataframe with BG and TRACT dataframes befor multiplying
exclude_columns_BG = ['COUNTY', 'BG', 'TRACT', 'STATE', 'NAME', 'REGION', 'P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH', 'P_MODE_NA']
include_columns_TRACT = ['P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH', 'P_MODE_NA'] 
## for BG
for variables in each_state_df.columns:
    if variables not in exclude_columns_BG:
       control_tot_BG[variables] *= each_state_df[variables][0]

## for TRACT
for variables in each_state_df.columns:
    if variables in include_columns_TRACT:
        control_tot_TRACT[variables] *= each_state_df[variables][0]

#### 
control_tot_BG[bg_hh_col] = control_tot_BG[bg_hh_col].round().astype(int)
control_tot_BG[bg_per_col] = control_tot_BG[bg_per_col].round().astype(int)
control_tot_STATE[st_hh_col] = control_tot_STATE[st_hh_col].round().astype(int)
control_tot_STATE[st_per_col] = control_tot_STATE[st_per_col].round().astype(int)
# control_tot_TRACT[tc_hh_col] = control_tot_TRACT[tc_hh_col].round().astype(int)
control_tot_TRACT[tc_per_col] = control_tot_TRACT[tc_per_col].round().astype(int)
scaled_control[scale_hh_col] = scaled_control[scale_hh_col].round().astype(int)
scaled_control[scale_per_col] = scaled_control[scale_per_col].round().astype(int)

control_tot_BG[['H_TOTAL','H_CHILDREN','P_TOTAL']].sum()
seed_hh['WGTP'].sum()
seed_per['PWGTP'].sum()

dataframes = {
     'seed_households.csv': seed_hh,
     'seed_persons.csv': seed_per,
     'control_totals_BG.csv': control_tot_BG,
     'control_totals_STATE.csv': control_tot_STATE,
     'control_totals_TRACT.csv': control_tot_TRACT,
     'scaled_control_totals_meta.csv': scaled_control
}    

## Save to the folder
def save_to_csv(df, file_name, state_path):
     output_path = os.path.join(main_folder_path, state_path, file_name)
     df.to_csv(output_path, index=False)

for file_name, df in dataframes.items():
    save_to_csv(df, file_name, STATE)


