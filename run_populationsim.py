# ActivitySim
# See full license in LICENSE.txt.

import os
import sys
import argparse

# Need to include all these modules to ensure they are registered
# Otherwise injectables will not be found
from activitysim.core.config import setting
from activitysim.core import inject
from activitysim.cli.run import add_run_args, run
from populationsim import steps
from itertools import islice

from setup import settings, prepare_data, utils
from us import states

@inject.injectable()
def log_settings():

    return [
        'multiprocess',
        'num_processes',
        'resume_after',
        'GROUP_BY_INCIDENCE_SIGNATURE',
        'INTEGERIZE_WITH_BACKSTOPPED_CONTROLS',
        'SUB_BALANCE_WITH_FLOAT_SEED_WEIGHTS',
        'meta_control_data',
        'control_file_name',
        'USE_CVXPY',
        'USE_SIMUL_INTEGERIZER'
    ]

BATCH_SIZE = 4

if __name__ == '__main__':

    assert inject.get_injectable('preload_injectables')

    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
        
    args.config = []
    args.config += ['populationsim/configs_mp']
    args.config += ['populationsim/configs']
    args.data = 'populationsim/data'
    args.output = 'populationsim/output'
    
    if 'populationsim/configs_mp' in args.config:
        args.output += '_mp'    
        
        
    DataCreator = prepare_data.CreateInputData(replace=False, verbose=False)
    
    args_dict = {}
    for states_chunk in utils.batched(settings.STATES, BATCH_SIZE):
        fips = [getattr(states.lookup(s), 'fips') for s in states_chunk]
        state_str = '-'.join(states_chunk)
        
        new_args = {
            'data': os.path.join(args.data, state_str), 
            'output': os.path.join(args.output, state_str)
        }
        args_dict[state_str] = new_args
        
        if not os.path.exists(new_args['data']):
            os.makedirs(new_args['data'], exist_ok=True)
        if not os.path.exists(new_args['output']):
            os.makedirs(new_args['output'], exist_ok=True)
        
        DataCreator.create_inputs(
            FIPS=fips,
            STATES=list(states_chunk)
            )
    
    # Free up memory
    del DataCreator
    
    for state, kwargs in args_dict.items():
        args.data = kwargs['data']
        args.output = kwargs['output']
    
        try:
            print(f'#### Running PopulationSim for {state}... ####')
            sys.exit(run(args))
        except:
            print(f'Error running {state}, continuing...')
            continue