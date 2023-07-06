import pandas as pd
import geopandas as gpd

import data
import geographies
from utils import format_geoids
import settings
import os

### Aggregate special ACS targets ###
def create_acs_targets(replace=False):    
    print('#### Creating ACS targets... ####')
    
    paths = [f'{settings.POPSIM_DIR}/data/control_totals_{geo}.csv' for geo in settings.ACS_GEO_FIELDS.keys()]
    paths.append(f'{settings.POPSIM_DIR}/data/scaled_control_totals_meta.csv')
    if not replace and all([os.path.exists(path) for path in paths]):
        print('ACS targets already exist, skipping...')
        return    
    
    # Whole data set
    ACS_DATA = data.fetch('ACS')
    
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
    totals['REGION'] = 1
    totals_path = f'{settings.POPSIM_DIR}/data/scaled_control_totals_meta.csv'
    pd.DataFrame(totals, index=[0]).to_csv(totals_path, index=False)
        
    # Update GEOIDs and save results as targets
    for geo, df in acs_data.items():
        renamer = dict(zip(df.columns, df.columns.str.upper().str.replace(' ','_')))
        df.rename(columns=renamer, inplace=True)        
        df.rename(columns={'BLOCK_GROUP': 'BG'}, inplace=True)
        
        df = format_geoids(df)
        df['REGION'] = 1
                    
        path = f'{settings.POPSIM_DIR}/data/control_totals_{geo}.csv'        
        df.to_csv(path, index=False)
            
    return
    

### This file aggregates the PUMS data to match ACS data. ###
def create_seeds(replace=False):
    print('#### Creating seed files... ####')
    
    hhpath = f'{settings.POPSIM_DIR}/data/seed_households.csv'
    perpath = f'{settings.POPSIM_DIR}/data/seed_persons.csv'
    if not replace and os.path.exists(hhpath) and os.path.exists(perpath):
        print('Seed files already exist, skipping...')
        return
    
    PUMS_DATA = data.fetch('PUMS')
    
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
    
    # Save to populationsim inputs data folder
    print('Saving HH seed file...')
    pums_hh.to_csv(hhpath)
    
    print('Saving PER seed file...')
    pums_per.to_csv(perpath)
    
    return

def create_crosswalk(replace=False):
    
    print('#### Creating crosswalk... ####')
    
    fpath = f'{settings.POPSIM_DIR}/data/geo_cross_walk.csv'
    if not replace and os.path.exists(fpath):
        print('Crosswalk already exists. Skipping...')
        return
    
    GEO_PUMA = geographies.fetch('PUMA')
    GEO_BG = geographies.fetch('BG')
        
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
    print('Finding any unmatched block groups and joining by distance...')
    orphans = bg[xwalk.index_right.isna().values]
    for row in orphans.itertuples():
        # Find the nearest PUMA within the state
        state_pumas = puma.loc[puma.STATEFP == row.STATEFP]
        distances = row.geometry.distance(state_pumas.geometry)
        nearest = distances.idxmin()
        
        if distances[nearest]*111139 < 1:
            xwalk.loc[row.Index, 'index_right'] = nearest
        else:
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
    xwalk = format_geoids(xwalk)
    
    print('Saving crosswalk...')
    xwalk.to_csv(fpath)
        
    return
    

if __name__ == '__main__':
    print(f'#### Creating POPSIM inputs for {len(settings.STATES)} States: ####\n{settings.STATES}')
    create_seeds(replace=True)
    create_acs_targets(replace=True)
    create_crosswalk(replace=True)