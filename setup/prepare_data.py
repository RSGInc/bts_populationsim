import pandas as pd
import geopandas as gpd

import data
import geographies
from utils import format_geoids
import settings
import os


# Replace data?
REPLACE = True

# Whole data set
ACS_DATA = data.fetch('ACS')
PUMS_DATA = data.fetch('PUMS')
GEO_PUMA = geographies.fetch('PUMA')
GEO_BG = geographies.fetch('BG')

# PATHS
label_map = {'HH': 'households', 'PER': 'persons'}
ACS_DATA_PATHS = {geo: f'{settings.POPSIM_DIR}/data/control_totals_{geo}.csv' for geo in settings.ACS_GEO_FIELDS.keys()}
ACS_DATA_PATHS['REGION'] = f'{settings.POPSIM_DIR}/data/scaled_control_totals_meta.csv'
PUMS_DATA_PATHS = {level: f'{settings.POPSIM_DIR}/data/seed_{label_map[level]}.csv' for level in settings.PUMS_FIELDS.keys()}
XWALK_PATH = f'{settings.POPSIM_DIR}/data/geo_cross_walk.csv'

# Output variables
ACS_DATA_FINAL = {}
PUMS_DATA_FINAL = {}
XWALK_FINAL = pd.DataFrame()
  
def create_seeds(replace=REPLACE):
    global PUMS_DATA_FINAL
    
    print('#### Creating seed files... ####')
    
    if not replace and all([os.path.exists(path) for path in PUMS_DATA_PATHS.values()]):  
        print('Seed files already exist, skipping...')
        return
    
    # Select the states
    fips_int = [int(x) for x in settings.FIPS]
    pums_select = {geo: df[df.ST.isin(fips_int)] for geo, df in PUMS_DATA.items()}   
    
    # Join together to ensure that the same households are selected
    print('Joining HH and PER PUMS data for aggregation...')
    pums = pd.merge(pums_select['HH'], pums_select['PER'], on=['SERIALNO', 'ST', 'PUMA'])

    # Concatenate state and PUMA to create a unique PUMA identifier
    pums.rename(columns={'ST': 'STATE'}, inplace=True)
    pums = format_geoids(pums)    
    pums['REGION'] = 1
    
    # Create a new unique integer household_id column
    print('Creating household_id column...')
    pums['hh_id'] = pums.groupby(['SERIALNO', 'PUMA']).ngroup() + 1
    pums['hh_id'] = pums.PUMA.astype('int64') * (10 ** 8) + pums['hh_id']
        
    # Separate the household and person dataframes back out
    print('Separating HH and PER PUMS data...')
    percols = list(settings.PUMS_FIELDS['PER'].keys()) + ['REGION', 'STATE']
    hhcols = list(settings.PUMS_FIELDS['HH'].keys()) + ['hh_id', 'REGION', 'STATE']
    hhcols.remove('ST')
    percols.remove('ST')
    pums_hh = pums[hhcols].groupby('hh_id').first()
    pums_per = pums.set_index('hh_id')[percols]

    # Add num adults
    print('Calculating number of adults to households...')
    pums_hh = pums_hh.join(pums_per[pums_per.AGEP >= 18].groupby('hh_id').size().to_frame('NP_ADULTS'))
    pums_hh.NP_ADULTS.fillna(0, inplace=True)
    pums_hh.NP_ADULTS = pums_hh.NP_ADULTS.astype(int)    
    
    # Assert that index is consistent
    assert len(pums_hh) == pums_hh.index.nunique(), 'Index is not unique for HH!'
    
    PUMS_DATA_FINAL['HH'] = pums_hh
    PUMS_DATA_FINAL['PER'] = pums_per
    
    return

def create_acs_targets(replace=REPLACE):
    global ACS_DATA_FINAL
    
    print('#### Creating ACS targets... ####')
        
    if not replace and all([os.path.exists(path) for path in ACS_DATA_PATHS.values()]):
        print('ACS targets already exist, skipping...')
        return
        
    # Select the states
    acs_data_select = {geo: df[df.state.isin(settings.FIPS)].copy() for geo, df in ACS_DATA.items()}    
    
    # Make a new copy to work with
    acs_data = acs_data_select.copy()
    totals = {}
    for (geo, variable), fields in settings.ACS_AGGREGATION.items():
        acs_data[geo][variable] = acs_data_select[geo][fields].sum(axis=1)
        acs_data[geo].drop(columns=fields, inplace=True)
        totals[variable] = acs_data[geo][variable].sum()       
    
    # Prepare regional totals
    ACS_DATA_FINAL['REGION'] = pd.DataFrame(totals, index=pd.Index([1], name='REGION'))
        
    # Update GEOIDs and save results as targets
    for geo, df in acs_data.items():
        print(f'Formatting {geo} targets...')
        # Rename any specific columns
        renamer = dict(zip(df.columns, df.columns.str.upper()))
        for old_name, new_name in renamer.items():
            if old_name.upper() in settings.RENAME.keys():
                renamer[old_name] = settings.RENAME[new_name]
        df = df.rename(columns=renamer)
        # Format GEOIDs        
        df = format_geoids(df)
        df['REGION'] = 1
        
        # Assert that index is consistent
        assert len(df.index) == df[geo].nunique(), f'Index is not unique for {geo}!'
        
        ACS_DATA_FINAL[geo] = df         
        
    return
  
def create_crosswalk(replace=REPLACE):
    global XWALK_FINAL
    
    print('#### Creating crosswalk... ####')
    
    if not replace and os.path.exists(XWALK_PATH):
        print('Crosswalk already exists. Skipping...')
        return
        
    # Select the states
    puma_select = GEO_PUMA[GEO_PUMA.STATEFP.isin(settings.FIPS)]
    bg_select = GEO_BG[GEO_BG.STATEFP.isin(settings.FIPS)]    

    assert isinstance(puma_select, gpd.GeoDataFrame), 'PUMA data is not a GeoDataFrame!'
    assert isinstance(bg_select, gpd.GeoDataFrame), 'BG data is not a GeoDataFrame!'
        
    # Perform a spatial join on the PUMA and BG geometries to create a xwalk
    puma = puma_select.set_index('GEOID')
    bg = bg_select.set_index('GEOID')[['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE', 'NAMELSAD', 'geometry']]
        
    assert bg.crs == puma.crs, 'CRS do not match!'
    
    # Using centroids to perform spatial join because it is faster and avoids ambiguous intersections
    print('Extracting centroids...')
    bg_centroid = bg.copy()
    bg_centroid.geometry = bg.geometry.to_crs('+proj=cea').centroid.to_crs(bg.crs)

    # Spatial join    
    print('Performing spatial join on centroids...')
    # xwalk = gpd.sjoin(bg, puma, how='left', predicate='within').reset_index()
    xwalk = gpd.sjoin(bg_centroid, puma[['geometry']], how='left', predicate='within')
        
    # Find any unmatched block groups and joint by distance, if possible
    orphans = bg[xwalk.index_right.isna().values]
    print(f'Found {orphans.shape[0]} any unmatched block groups and joining by distance (<1km)...')
    for row in orphans.itertuples():
        # Find the nearest PUMA within the state
        state_pumas = puma.loc[puma.STATEFP == row.STATEFP]
        distances = row.geometry.distance(state_pumas.geometry)
        nearest = distances.idxmin()
        
        # Approximate degrees to meters conversion
        if distances[nearest]*111139 < 1000:
            print(f'Found a PUMA within {distances[nearest]*111139}m for {row.Index}! Joining...')
            xwalk.loc[row.Index, 'index_right'] = nearest
        else:
            print(f'Could not find a PUMA within 1km for {row.Index}! Dropping...')
            xwalk.drop(row.Index, inplace=True)
        
    # Create dataframe to save as csv
    names = {
        'index_right': 'PUMA',
        'GEOID': 'BG', 
        'STATEFP': 'STATE',
        'COUNTYFP': 'COUNTY', 
        'TRACTCE': 'TRACT'
    }
    
    xwalk.reset_index(inplace=True)
    xwalk['REGION'] = 1
    xwalk.drop(columns=['geometry'], inplace=True)
    xwalk.rename(columns=names, inplace=True)
    
    # Format geoids
    XWALK_FINAL = format_geoids(xwalk)
        
    # Cross-check that all BGs and Tracts have data
    _data = {**ACS_DATA, **{'PUMA': PUMS_DATA['HH']}}
    for geo, df in _data.items():        
        
        # Rename any specific columns
        renamer = dict(zip(df.columns, df.columns.str.upper()))
        for old_name, new_name in renamer.items():
            if old_name.upper() in settings.RENAME.keys():
                renamer[old_name] = settings.RENAME[new_name]
        df = df.rename(columns=renamer)
        
        # Format geoids
        df = format_geoids(df, verbose=False)
        
        # Remove any missing geographies
        is_in = XWALK_FINAL[geo].isin(df[geo].unique())
        
        print(f'Removing {sum(~is_in)} {geo} from crosswalk with missing data...')
        XWALK_FINAL = XWALK_FINAL[is_in]
            
    return

def save_inputs(replace=REPLACE):

    if len(PUMS_DATA_FINAL) > 0 and replace:
        for level, path in PUMS_DATA_PATHS.items():
            print(f'Saving {level} PUMS data...')
            PUMS_DATA_FINAL[level].to_csv(path, index=True)

    if len(ACS_DATA_FINAL) > 0 and replace:
        for geo, path in ACS_DATA_PATHS.items():
            print(f'Saving {geo} ACS data...')
            ACS_DATA_FINAL[geo].to_csv(path, index=False)
        
    if not XWALK_FINAL.empty and replace:
        print('Saving crosswalk...')
        XWALK_FINAL.to_csv(XWALK_PATH, index=False)

if __name__ == '__main__':
    print(f'#### Creating POPSIM inputs for {len(settings.STATES)} States: ####\n{settings.STATES}')
    create_seeds()
    create_acs_targets()
    create_crosswalk()
    save_inputs()