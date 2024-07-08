import os
import requests
import json
import pandas as pd
import numpy as np

api_key = 'your key'

state_fips = '46'


def fetch_acs_data(year, dataset, state_fips, variables, api_key):
    url = f'https://api.census.gov/data/{year}/acs/{dataset}/pums?get={variables},ST,PUMA&for=public%20use%20microdata%20area:*&in=state:{state_fips}&key={api_key}'
    response = requests.get(url)
    
    if response.status_code == 200:
        # Load the JSON response
        census_data = response.json()
        df = pd.DataFrame(census_data[1:], columns=census_data[0])
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
df_1yr_wgtp = fetch_acs_data(year='2021', dataset='acs1', state_fips=state_fips, variables='WGTP', api_key=api_key)
df_1yr_wgtp = df_1yr_wgtp.apply(pd.to_numeric, errors = 'ignore')
df_1yr_wgtp.drop(columns=["state", "public use microdata area"], inplace=True)

## summation of 1-year WGTP for each PUMA
sum_1yr_wgtp_puma = df_1yr_wgtp.groupby('PUMA')['WGTP'].sum().reset_index()
sum_1yr_wgtp_puma.rename(columns={'WGTP':'PUMA_SUM_1yr'}, inplace=True)

## summation of 1-year WGTP for state
sum_1yr_wgtp_state = df_1yr_wgtp['WGTP'].sum()

### for 5year data
df_5yr_wgtp = fetch_acs_data(year='2021', dataset='acs5', state_fips=state_fips, variables='WGTP', api_key=api_key)
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
df_1yr_pwgtp = fetch_acs_data(year='2021', dataset='acs1', state_fips=state_fips, variables='PWGTP', api_key=api_key)
df_1yr_pwgtp = df_1yr_pwgtp.apply(pd.to_numeric, errors = 'ignore')
df_1yr_pwgtp.drop(columns=["state", "public use microdata area"], inplace=True)


sum_1yr_wgtp_puma = df_1yr_pwgtp.groupby('PUMA')['PWGTP'].sum().reset_index()
sum_1yr_wgtp_puma.rename(columns={'PWGTP':'PUMA_SUM_1yr'}, inplace= True)

sum_1yr_wgtp_state = df_1yr_pwgtp['PWGTP'].sum()

df_5yr_pwgtp = fetch_acs_data(year='2021', dataset='acs5', state_fips=state_fips, variables='PWGTP', api_key=api_key)
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


##multiply with seed_households and seed_person
seed_hh = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\seed_households.csv')
seed_per = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\seed_persons.csv')

seed_hh = pd.merge(seed_hh, wgtp_puma_adj_factor, on='PUMA', how='left')
seed_hh['WGTP'] = seed_hh['WGTP'] * seed_hh['ADJ_FACTOR_HH']
seed_hh.drop(columns=['ADJ_FACTOR_HH'], inplace=True)

seed_per = pd.merge(seed_per, pwgtp_puma_adj_factor, on='PUMA', how='left')
seed_per['PWGTP'] = seed_per['PWGTP'] * seed_per['ADJ_FACTOR_PER']
seed_per.drop(columns=['ADJ_FACTOR_PER'], inplace=True)

seed_hh.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\seed_households.csv', index=False)
seed_per.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\seed_persons.csv', index=False)


#####################################################################################
control_tot_BG = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_BG.csv')
control_tot_STATE = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_STATE.csv')
control_tot_TRACT = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_TRACT.csv')
scaled_control = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\scaled_control_totals_meta.csv')
geo_crosswalk = pd.read_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\geo_cross_walk.csv')

control_tot_BG['H_TOTAL'].sum()

BG_ID = geo_crosswalk[['BG', 'PUMA']]
TRACT_ID = geo_crosswalk[['TRACT', 'PUMA']]
TRACT_ID = TRACT_ID.drop_duplicates(subset='TRACT', ignore_index= True)

adjustment_factor_BG = pd.merge(BG_ID, wgtp_puma_adj_factor, on= 'PUMA', how= 'left')
adjustment_factor_BG = pd.merge(adjustment_factor_BG, pwgtp_puma_adj_factor, on= 'PUMA', how= 'left')
control_tot_BG = control_tot_BG.merge(adjustment_factor_BG, on='BG', how='left')


adjustment_factor_TRACT = pd.merge(TRACT_ID, wgtp_puma_adj_factor, on= 'PUMA', how= 'left')
adjustment_factor_TRACT = pd.merge(adjustment_factor_TRACT, pwgtp_puma_adj_factor, on= 'PUMA', how= 'left')
control_tot_TRACT = control_tot_TRACT.merge(adjustment_factor_TRACT, on='TRACT', how='left')

bg_hh_col = ['H_TOTAL',
             'H_CHILDREN', 'H_NO_CHILDREN',
             'H_INCOME_0_25', 'H_INCOME_25_50', 'H_INCOME_50_75', 'H_INCOME_75_100', 'H_INCOME_100_150', 'H_INCOME_150PLUS']

bg_per_col = ['P_TOTAL', 
              'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64', 'P_AGE_65PLUS', 
              'P_MALE', 'P_FEMALE', 
              'P_HISPANIC', 'P_NON_HISPANIC',
              'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_AAPI', 'P_RACE_OTHER',  
              'P_FULL_TIME', 'P_PART_TIME', 'P_NON_WORKER', 
              'P_UNIVERSITY', 'P_NON_UNIVERSITY']

st_hh_col = ['H_TOTAL', 
             'H_CHILDREN', 'H_NO_CHILDREN', 
             'H_INCOME_0_25',  'H_INCOME_25_50', 'H_INCOME_50_75','H_INCOME_75_100', 'H_INCOME_100_150','H_INCOME_150PLUS',
             'H_SIZE_1', 'H_SIZE_2', 'H_SIZE_3', 'H_SIZE_4PLUS', 
             'H_NO_VEH','H_VEH_1','H_VEH_2', 'H_VEH_3', 'H_VEH_4MORE']

st_per_col = ['P_TOTAL',
              'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64','P_AGE_65PLUS', 
              'P_FEMALE', 'P_MALE',
              'P_HISPANIC', 'P_NON_HISPANIC',
              'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_AAPI', 'P_RACE_OTHER',  
              'P_FULL_TIME', 'P_PART_TIME',
              'P_UNIVERSITY',
              'P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH']

tc_hh_col = ['H_SIZE_1', 'H_SIZE_2', 'H_SIZE_3', 'H_SIZE_4PLUS', 
             'H_NO_VEH', 'H_VEH_1', 'H_VEH_2', 'H_VEH_3', 'H_VEH_4MORE']

tc_per_col = ['P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH', 'P_MODE_NA']

scale_hh_col = ['H_TOTAL', 
                'H_CHILDREN', 'H_NO_CHILDREN', 
                'H_INCOME_0_25',  'H_INCOME_25_50', 'H_INCOME_50_75','H_INCOME_75_100', 'H_INCOME_100_150','H_INCOME_150PLUS',
                'H_SIZE_1', 'H_SIZE_2', 'H_SIZE_3', 'H_SIZE_4PLUS', 
                'H_NO_VEH','H_VEH_1','H_VEH_2', 'H_VEH_3', 'H_VEH_4MORE']

scale_per_col = ['P_TOTAL',
                'P_AGE_0_4', 'P_AGE_5_17', 'P_AGE_18_34', 'P_AGE_35_49', 'P_AGE_50_64','P_AGE_65PLUS', 
                'P_FEMALE', 'P_MALE', 
                'P_HISPANIC', 'P_NON_HISPANIC',
                'P_RACE_WHITE', 'P_RACE_BLACK', 'P_RACE_AAPI', 'P_RACE_OTHER',  
                'P_FULL_TIME', 'P_PART_TIME',
                'P_UNIVERSITY',
                'P_MODE_AUTO_OTHER', 'P_MODE_TRANSIT', 'P_MODE_WALK_BIKE', 'P_MODE_WFH']

scaled_control['H_TOTAL'].sum()
scaled_control['P_TOTAL'].sum()
control_tot_BG[bg_hh_col] = control_tot_BG[bg_hh_col].apply(lambda x: x * control_tot_BG['ADJ_FACTOR_HH'], axis=0)
control_tot_BG[bg_hh_col] = control_tot_BG[bg_hh_col].round().astype(int)
control_tot_BG[bg_per_col] = control_tot_BG[bg_per_col].apply(lambda x: x * control_tot_BG['ADJ_FACTOR_PER'], axis=0)
control_tot_BG[bg_per_col] = control_tot_BG[bg_per_col].round().astype(int)

control_tot_STATE[st_hh_col] = control_tot_STATE[st_hh_col] * wgtp_state_adj_factor
control_tot_STATE[st_hh_col] = control_tot_STATE[st_hh_col].round().astype(int)
control_tot_STATE[st_per_col] = control_tot_STATE[st_per_col] * pwgtp_state_adj_factor
control_tot_STATE[st_per_col] = control_tot_STATE[st_per_col].round().astype(int)

control_tot_TRACT[tc_hh_col] = control_tot_TRACT[tc_hh_col].apply(lambda x: x * control_tot_TRACT['ADJ_FACTOR_HH'], axis=0)
control_tot_TRACT[tc_hh_col] = control_tot_TRACT[tc_hh_col].round().astype(int)
control_tot_TRACT[tc_per_col] = control_tot_TRACT[tc_per_col].apply(lambda x: x * control_tot_TRACT['ADJ_FACTOR_PER'], axis=0)
control_tot_TRACT[tc_per_col] = control_tot_TRACT[tc_per_col].round().astype(int)

scaled_control[scale_hh_col] = scaled_control[scale_hh_col] * wgtp_state_adj_factor
scaled_control[scale_hh_col] = scaled_control[scale_hh_col].round().astype(int)
scaled_control[scale_per_col] = scaled_control[scale_per_col] * pwgtp_state_adj_factor
scaled_control[scale_per_col] = scaled_control[scale_per_col].round().astype(int)








control_tot_BG.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_BG.csv', index=False)
control_tot_STATE.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_STATE.csv', index=False)
control_tot_TRACT.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\control_totals_TRACT.csv', index=False)
scaled_control.to_csv('E:\\bts_populationsim_adib\\bts_populationsim\\populationsim\\data\\SD\\scaled_control_totals_meta.csv', index=False)