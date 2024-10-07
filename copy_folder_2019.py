import os
import shutil
import us
from us import states

system_path = os.getcwd()
system_path_populationsim = os.path.join(system_path, 'populationsim')
main_folder_path = os.path.join(system_path_populationsim, 'data')

states_folders = [state.abbr for state in us.states.STATES] + ["DC"]

data_2021_5yr = os.path.join(main_folder_path, 'data_2021_5yr')
# if not os.path.exists(data_2021_5yr):
#     os.makedirs(data_2021_5yr)

### move 2021 1 year data for future use
data_2021_1yr = os.path.join(main_folder_path, 'data_2021_1yr')
if not os.path.exists(data_2021_1yr):
    os.makedirs(data_2021_1yr)

for state in states_folders:
    source_path_2021_1yr = os.path.join(main_folder_path, state)
    destination_path_2021_1yr = os.path.join(data_2021_1yr, state)
    
    if os.path.exists(source_path_2021_1yr) and os.path.isdir(source_path_2021_1yr):
        shutil.move(source_path_2021_1yr, destination_path_2021_1yr)

## again copy 2021 5-year data for 2019 1-year adjustment
for state in states_folders:
    source_path_2019 = os.path.join(data_2021_5yr, state)
    destination_path_2019 = os.path.join(main_folder_path, state) 
    if os.path.exists(source_path_2019) and os.path.isdir(source_path_2019):
        shutil.copytree(source_path_2019, destination_path_2019)