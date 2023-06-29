import prepare_data

def main():
    prepare_data.create_acs_targets()
    prepare_data.create_seeds()
    prepare_data.create_crosswalk()
    
main()