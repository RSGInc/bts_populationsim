# Author: Nicholas Fournier
#
# This is a run script that prepares the input data if not already prepared, 
# and then runs the PopulationSim model for a batch of states.
#
# It calls the run_populationsim.py script separately for each batch of states, 
# otherwise importing the activitysim pipeline would get corrupted in the loop.


import re
import os
import argparse
import subprocess
import sys
from copy import copy
from setup_inputs.prepare_data import CreateInputData
from setup_inputs import settings, utils
from validation.validate_populationsim import Validation

# Create a namespace object to hold our args
parser = argparse.ArgumentParser()
parser.add_argument('--config', type=str)
parser.add_argument('--data', type=str)
parser.add_argument('--output', type=str)

popsim_dir = os.path.join(os.path.dirname(__file__), 'populationsim')

# Find the python path string to the current virtual environment to pass to subprocess
python_path = sys.executable

def cleanup_output(output_dir):
    for x in os.listdir(output_dir):
        if x.endswith('.h5'):
            print('Removing extra pipeline file')
            os.remove(f"{output_dir}/{x}")


if __name__ == '__main__':
    
    base_args = parser.parse_args()    
    base_args.config = [os.path.join(popsim_dir, x) for x in ['configs_mp', 'configs']]
    base_args.data = os.path.join(popsim_dir, 'data')
    base_args.output = os.path.join(popsim_dir, 'output')          
        
    if any([True for x in base_args.config if 'configs_mp' in x]):
        base_args.output += '_mp'
        
    # Expected inputs
    expected_inputs = [
        'scaled_control_totals_meta.csv', 
        'seed_households.csv',
        # 'control_totals_.*.csv', 
        'control_totals_BG.csv',
        'control_totals_TRACT.csv',
        'control_totals_STATE.csv',
        'geo_cross_walk.csv', 
        'seed_persons.csv', 
        ]
    
    DataCreator = None

    # settings.STATES = ['AK', 'WY'] # Debugging
    for states_chunk in utils.batched(settings.STATES_AND_TERRITORIES, settings.BATCH_SIZE):
        
        if len(states_chunk) > 12:
            state_str_range = states_chunk[0] + '-' + states_chunk[-1]            
            state_str = f'{settings.BATCH_SIZE}_states_{state_str_range}'
            print(f'#### Batch run of PopulationSim for {settings.BATCH_SIZE} states: {state_str_range}... ####')
            
        else:
            state_str = '-'.join(states_chunk)            
            print(f'#### Batch run of PopulationSim for {state_str}... ####')           

        # Current args
        args = copy(base_args)
        args.data += f'/{state_str}'
        args.output += f'/{state_str}'
        
        # Check for existing data if string matches regex list
        existing_inputs = False
        try:
            found = [any([bool(re.search(regex, x)) for x in os.listdir(args.data)]) for regex in expected_inputs]
            if found and all(found):
                existing_inputs = True
        except:
            existing_inputs = False
        
        # If not existing, create data                
        if not existing_inputs:
            if not DataCreator:
                DataCreator = CreateInputData(replace=False, verbose=False)

            DataCreator.create_inputs(
                STATES_AND_TERRITORIES=list(states_chunk),
                data_dir=os.path.join(settings.POPSIM_DIR, 'data', state_str)
            )
        else:
            print(f'#### {state_str} data already exists... ####')
                 
        try:            
            existing_outputs = os.path.exists(os.path.join(args.output, 'final_expanded_household_ids.csv'))
            if not existing_outputs:  
                print(f'#### Running PopulationSim for {state_str}... ####')
                
                if not os.path.exists(args.output):
                    os.makedirs(args.output, exist_ok=True)
                
                command = [python_path, '-m', 'run_populationsim']
                for k, arg in vars(args).items():                    
                    arg = [arg] if isinstance(arg, str) else arg                    
                    for subarg in arg:
                        command.append('--' + k)
                        command.append(subarg)             
                                                
                subprocess.call(command)
                # cleanup_output(args.output)
                
            else:
                print(f'#### {state_str} already run, skipping... ####')
                # cleanup_output(args.output)
        
            print(f'#### Running validation for {state_str}... ####')
            validation = Validation(args.config[1])
            validation.run_validation()
                
        except:
            print(f'Error running {state_str}, skipping...')
            continue
        
    
