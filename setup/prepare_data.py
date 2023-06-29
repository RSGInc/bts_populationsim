import pandas as pd
import geopandas as gpd

import data
import geographies
from utils import format_geoids
import settings


### Aggregate special ACS targets ###
def create_acs_targets():
    ACS_DATA = data.fetch('ACS')
    
    print('Creating ACS targets...')
    # Make a new copy to work with
    acs_data = ACS_DATA.copy()
    totals = {}
    for (geo, variable), fields in settings.ACS_AGGREGATION.items():
        acs_data[geo][variable] = ACS_DATA[geo][fields].sum(axis=1)
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
def create_seeds():
    PUMS_DATA = data.fetch('PUMS')
    print('Creating seed files...')
    
    # Join together to ensure that the same households are selected
    pums = pd.merge(PUMS_DATA['HH'], PUMS_DATA['PER'], on=['SERIALNO', 'state', 'public use microdata area'])

    # Concatenate state and PUMA to create a unique PUMA identifier
    pums.rename(columns={'state': 'STATE'}, inplace=True)
    pums = format_geoids(pums)    
    pums['REGION'] = 1
    
    # Create a new unique integer household_id column
    pums['hh_id'] = (pums.groupby('SERIALNO').ngroup() + 1).astype(str).str.zfill(8)
    pums['hh_id'] = pums.PUMA.astype(str) + pums.hh_id
        
    # Separate the household and person dataframes back out
    hhcols = list(settings.PUMS_FIELDS['HH'].keys()) + ['hh_id', 'REGION']
    percols = list(settings.PUMS_FIELDS['PER'].keys()) + ['REGION']
    pums_hh = pums[hhcols].groupby('hh_id').first()
    pums_per = pums.set_index('hh_id')[percols]

    # Add num adults
    pums_hh = pums_hh.join(pums_per[pums_per.AGEP >= 18].groupby('hh_id').size().to_frame('NP_ADULTS'))
    pums_hh.NP_ADULTS.fillna(0, inplace=True)
    pums_hh.NP_ADULTS = pums_hh.NP_ADULTS.astype(int)
    
    # Save to populationsim inputs data folder
    pums_hh.to_csv(f'{settings.POPSIM_DIR}/data/seed_households.csv')
    pums_per.to_csv(f'{settings.POPSIM_DIR}/data/seed_persons.csv')
    
    return

def create_crosswalk():    
    GEO_PUMA = geographies.fetch('PUMA')
    GEO_BG = geographies.fetch('BG')
    
    print('Creating crosswalk...')
    # Perform a spatial join on the PUMA and BG geometries to create a xwalk
    puma = GEO_PUMA.set_index('GEOID')
    bg = GEO_BG.set_index('GEOID')[['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE', 'NAMELSAD', 'geometry']]
        
    assert bg.crs == puma.crs, 'CRS do not match!'
    
    # Using centroids to perform spatial join because it is faster and avoids ambiguous intersections
    bg_centroid = bg.copy()
    bg_centroid.geometry = bg.geometry.to_crs('+proj=cea').centroid.to_crs(bg.crs)

    # Spatial join    
    # xwalk = gpd.sjoin(bg, puma, how='left', predicate='within').reset_index()
    xwalk = gpd.sjoin(bg_centroid, puma[['geometry']], how='left', predicate='within')
        
    # Find any unmatched block groups and joint by distance, if possible
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
    xwalk.to_csv(f'{settings.POPSIM_DIR}/data/geo_cross_walk.csv', index=True)
        
    return
    

if __name__ == '__main__':
    create_acs_targets()
    create_seeds()
    create_crosswalk()