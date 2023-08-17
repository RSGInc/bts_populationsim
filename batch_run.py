import re
import os
import argparse
import subprocess
from copy import copy
from setup_inputs.prepare_data import CreateInputData
from setup_inputs import settings, utils

# Create a namespace object to hold our args
parser = argparse.ArgumentParser()
parser.add_argument('--config', type=str)
parser.add_argument('--data', type=str)
parser.add_argument('--output', type=str)


def cleanup_output(output_dir):
    for x in os.listdir(output_dir):
        if x.endswith('.h5'):
            print('Removing extra pipeline file')
            os.remove(f"{output_dir}/{x}")


if __name__ == '__main__':
    
    base_args = parser.parse_args()       
    base_args.config = ['populationsim/configs_mp', 'populationsim/configs']
    base_args.data = 'populationsim/data'
    base_args.output = 'populationsim/output'
        
    if 'populationsim/configs_mp' in base_args.config:
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

    settings.STATES = ['AK', 'WY', 'VT'] # Debugging
    for states_chunk in utils.batched(settings.STATES, settings.BATCH_SIZE):
        
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
                STATES=list(states_chunk),
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
                
                command = ['python', '-m', 'run_populationsim']
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
                
        except:
            print(f'Error running {state_str}, skipping...')
            continue
        
    
