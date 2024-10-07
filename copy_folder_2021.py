import os
import shutil
import us
from us import states

system_path = os.getcwd()
system_path_populationsim = os.path.join(system_path, 'populationsim')
main_folder_path = os.path.join(system_path_populationsim, 'data')

states_folders = [state.abbr for state in us.states.STATES] + ["DC"]

### copy 2021 5 year data for future use
data_2021_5yr = os.path.join(main_folder_path, 'data_2021_5yr')
if not os.path.exists(data_2021_5yr):
    os.makedirs(data_2021_5yr)

for state in states_folders:
    source_path = os.path.join(main_folder_path, state)
    destination_path = os.path.join(data_2021_5yr, state)
    
    if os.path.exists(source_path) and os.path.isdir(source_path):
        shutil.copytree(source_path, destination_path)
