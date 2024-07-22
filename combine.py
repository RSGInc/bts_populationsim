import os
import pandas as pd
import numpy as np


system_path = os.getcwd()
system_path_populationsim = os.path.join(system_path, 'populationsim')
data_folder_path = os.path.join(system_path_populationsim, 'data')
output_folder_path = os.path.join(system_path_populationsim, 'output_mp')

#states

states_folders = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", 
                  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
                  ]

## expanded HH ID's
expanded_household_ids = pd.DataFrame()

for state_folder in states_folders:
    file_path = os.path.join(output_folder_path, state_folder, 'final_expanded_household_ids.csv')
    if os.path.exists(file_path):
        temp_df = pd.read_csv(file_path)
        expanded_household_ids = pd.concat([expanded_household_ids, temp_df], ignore_index=True)
    else:
        print(f"File not found: {file_path}")

expanded_household_ids.to_csv(os.path.join(system_path_populationsim, 'final_expanded_household_ids_2019_1yearadj.csv'), index=False)

# seed HH and persons

# hh
seed_hh = pd.DataFrame()

for state_folder in states_folders:
    file_path = os.path.join(data_folder_path, state_folder, 'seed_households.csv')
    if os.path.exists(file_path):
        temp_df = pd.read_csv(file_path)
        seed_hh = pd.concat([seed_hh, temp_df], ignore_index=True)
    else:
        print(f"File not found: {file_path}")

seed_hh.to_csv(os.path.join(system_path_populationsim, 'seed_households_2019_1yearadj.csv'), index=False)


# person
seed_per = pd.DataFrame()

for state_folder in states_folders:
    file_path = os.path.join(data_folder_path, state_folder, 'seed_persons.csv')
    if os.path.exists(file_path):
        temp_df = pd.read_csv(file_path)
        seed_per = pd.concat([seed_per, temp_df], ignore_index=True)
    else:
        print(f"File not found: {file_path}")

seed_per.to_csv(os.path.join(system_path_populationsim, 'seed_persons_2019_1yearadj.csv'), index=False)

