import pandas as pd
import geopandas as gpd
import os
from us import states
from setup_inputs import settings, utils, geographies, fetch

ARC_METERS = 111139

class CreateInputData:
    def __init__(self, replace: bool = True, verbose: bool = True) -> None:
                
        self.FIPS = settings.FIPS
        self.STATES = settings.STATES
        self.replace = replace
        self.verbose = verbose
        
        # Raw data
        self.ACS_DATA = fetch.fetch('ACS')
        self.PUMS_DATA = fetch.fetch('PUMS')        
        self.GEO_BG = geographies.fetch('BG')
        self.GEO_TRACT = geographies.fetch('TRACT')
        self.GEO_PUMA = geographies.fetch('PUMA')
        
        # Output variables
        self.ACS_DATA_FINAL = {}
        self.PUMS_DATA_FINAL = {}
        self.XWALK_FINAL = pd.DataFrame()
            
    def create_inputs(self, STATES: list = settings.STATES, data_dir = None):
        
        self.skip_pums = False
        self.skip_acs = False
        self.skip_xwalk = False
        
        # Updated FIPS and STATES to create
        self.STATES = STATES
        self.FIPS = [getattr(states.lookup(s), 'fips') for s in STATES]
        
        # PATHS
        if not data_dir:
            data_dir = os.path.join(settings.POPSIM_DIR, 'data', '-'.join(self.STATES))
            
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
        
        label_map = {'HH': 'households', 'PER': 'persons'}
        geo_list = list(settings.ACS_GEO_FIELDS.keys()) + ['STATE']
        self.ACS_DATA_PATHS = {geo: f'{data_dir}/control_totals_{geo}.csv' for geo in geo_list}
        self.ACS_DATA_PATHS['REGION'] = f'{data_dir}/scaled_control_totals_meta.csv'
        self.PUMS_DATA_PATHS = {level: f'{data_dir}/seed_{label_map[level]}.csv' for level in settings.PUMS_FIELDS.keys()}
        self.XWALK_PATH = f'{data_dir}/geo_cross_walk.csv'
        
        print(f'#### Creating POPSIM inputs for {len(self.STATES)} States: ####\n{self.STATES}')
        # Check which fiels need updating        
        if not self.replace and all([os.path.exists(path) for path in self.PUMS_DATA_PATHS.values()]):  
            print('Seed files already exist, skipping...')
            self.skip_pums = True                    
        
        if not self.replace and all([os.path.exists(path) for path in self.ACS_DATA_PATHS.values()]):
            print('ACS targets already exist, skipping...')
            self.skip_acs = True

        if not self.replace and os.path.exists(self.XWALK_PATH):
            print('Crosswalk already exists. Skipping...')
            self.skip_xwalk = True
        
        if not self.skip_pums:
            self.create_seeds()
            
        if not self.skip_acs:
            self.create_acs_targets()
            
        if not self.skip_xwalk:
            self.create_crosswalk()
        
        self.save_inputs()

    def create_seeds(self):        
        print('#### Creating seed files... ####')
        
        # Select the states
        fips_int = [int(x) for x in self.FIPS]
        pums_select = {geo: df[df.ST.isin(fips_int)].copy() for geo, df in self.PUMS_DATA.items()}   
        
        # Join together to ensure that the same households are selected
        print('Joining HH and PER PUMS data for aggregation...')
        pums = pd.merge(pums_select['HH'], pums_select['PER'], on=['SERIALNO', 'ST', 'PUMA'])

        # Concatenate state and PUMA to create a unique PUMA identifier
        pums.rename(columns={'ST': 'STATE'}, inplace=True)
        pums = utils.format_geoids(pums, verbose=self.verbose)
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
        
        self.PUMS_DATA_FINAL['HH'] = pums_hh
        self.PUMS_DATA_FINAL['PER'] = pums_per
        
        return

    def create_acs_targets(self):        
        print('#### Creating ACS targets... ####')
            
        # Select the states
        acs_data_select = {geo: df[df.state.isin(self.FIPS)].copy() for geo, df in self.ACS_DATA.items()}    
        
        # Make a new copy to work with
        acs_data = acs_data_select.copy()
        state_totals = {}
        region_totals = {}
        for (geo, variable), fields in settings.ACS_AGGREGATION.items():
            acs_data[geo][variable] = acs_data_select[geo][fields].sum(axis=1)
            acs_data[geo].drop(columns=fields, inplace=True)
            region_totals[variable] = acs_data[geo][variable].sum()
            state_totals[variable] = acs_data[geo].groupby('state')[variable].sum()
            
        
        # Prepate state totals
        self.ACS_DATA_FINAL['STATE'] = pd.DataFrame(state_totals)
        self.ACS_DATA_FINAL['STATE'].index.name = 'STATE'
        
        # Prepare regional totals
        self.ACS_DATA_FINAL['REGION'] = pd.DataFrame(region_totals, index=pd.Index([1], name='REGION'))
            
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
            df = utils.format_geoids(df, verbose=self.verbose)
            df['REGION'] = 1
            
            # Assert that index is consistent
            assert len(df.index) == df[geo].nunique(), f'Index is not unique for {geo}!'
            
            self.ACS_DATA_FINAL[geo] = df         
            
        return
    
    def create_crosswalk(self):
        
        print('#### Creating crosswalk... ####')

        fips_int = [int(x) for x in self.FIPS]
        fips_str = self.FIPS
        
        xwalk_ls = []
        # Loop through the states creating crosswalk then concatenate at end, this ensures PUMAS are not joined to wrong state
        # for [fips_int] in self.FIPS:
        # print(f'Creating crosswalk for {states.lookup(fips_int[0]).name}...')
        
        # Select the states
        puma_select = self.GEO_PUMA[self.GEO_PUMA.STATEFP.isin(fips_str)]
        bg_select = self.GEO_BG[self.GEO_BG.STATEFP.isin(fips_str)]
        tract_select = self.GEO_TRACT[self.GEO_TRACT.STATEFP.isin(fips_str)]            
        
        assert isinstance(puma_select, gpd.GeoDataFrame), 'PUMA data is not a GeoDataFrame!'
        assert isinstance(bg_select, gpd.GeoDataFrame), 'BG data is not a GeoDataFrame!'
        assert isinstance(tract_select, gpd.GeoDataFrame), 'Tract data is not a GeoDataFrame!'
        
        # Local copy to avoid corrupting original data
        puma_select = puma_select.copy()
        bg_select = bg_select.copy()
        tract_select = tract_select.copy()
            
        # Perform a spatial join on the PUMA and BG geometries to create a xwalk
        geoms = {
            'puma': puma_select.set_index('GEOID'),
            'tract': tract_select.set_index('GEOID'),
            'bg': bg_select.set_index('GEOID')[['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE', 'NAMELSAD', 'geometry']],
        }

        assert geoms['bg'].crs == geoms['tract'].crs == geoms['puma'].crs, 'CRS do not match!'
        
        # Using centroids to perform spatial join because it is faster and avoids ambiguous intersections
        print('Extracting centroids...')
        centroids = {
            'bg': geoms['bg'].copy(),
            'tract': geoms['tract'].copy(),
            'puma': geoms['puma'].copy(),
        }        
        for k, v in centroids.items():
            centroids[k].geometry = v.geometry.to_crs('+proj=cea').centroid.to_crs(geoms[k].crs)
                    
        # Spatial join
        print('Performing spatial join on centroids...')                
        xwalks = {}
        for k, v in centroids.items():
            if k.lower() == 'puma':
                continue
            print(f'Performing spatial join with PUMAs on {k} centroids...')
            xwalks[k] = gpd.sjoin(v, geoms['puma'][['geometry']], how='left', predicate='within')        
        
        # Find any unmatched block groups and joint by distance, if possible
        for geo, xwalk in xwalks.items():
            points = centroids[geo]
            xwalk = xwalks[geo]
            orphans = points[xwalk.index_right.isna().values]
            print(f'Found {orphans.shape[0]} any unmatched {geo} and joining by distance (<1km)...')
            for row in orphans.itertuples():
                # Find the nearest PUMA within the state
                state_pumas = geoms['puma'].loc[geoms['puma'].STATEFP == row.STATEFP]
                distances = row.geometry.distance(state_pumas.geometry)
                nearest = distances.idxmin()
                
                # Approximate degrees to meters conversion
                if distances[nearest]*ARC_METERS < 1000:
                    print(f'Found a PUMA within {distances[nearest]*ARC_METERS}m for {geo} {row.Index}! Joining...')
                    xwalk.loc[row.Index, 'index_right'] = nearest
                else:
                    print(f'Could not find a PUMA within 1km for {geo} {row.Index}! Dropping...')
                    xwalk.drop(row.Index, inplace=True)
            
            xwalks[geo] = xwalk
        
        names = {
            'index_right': 'PUMA',
            'GEOID': 'BG', 
            'STATEFP': 'STATE',
            'COUNTYFP': 'COUNTY', 
            'TRACTCE': 'TRACT'
        }
        # Format columns
        for geo, df in xwalks.items():            
            xwalk = df.reset_index(inplace=False)
            xwalk['REGION'] = 1
            xwalk.drop(columns=['geometry'], inplace=True)
            xwalk.rename(columns=names, inplace=True)  
            xwalks[geo] = utils.format_geoids(xwalk, verbose=self.verbose)
        
        
        # Use BG as the final crosswalk
        xwalk_final = xwalks['bg']
        
        # Cross-check that all BGs and Tracts have data
        _data = {**self.ACS_DATA, **{'PUMA': self.PUMS_DATA['HH']}}
        for geo, df in _data.items():
            
            # Rename any specific columns
            renamer = dict(zip(df.columns, df.columns.str.upper()))
            for old_name, new_name in renamer.items():
                if old_name.upper() in settings.RENAME.keys():
                    renamer[old_name] = settings.RENAME[new_name]
            df = df.rename(columns=renamer)
            
            # Format geoids
            df = utils.format_geoids(df, verbose=self.verbose)      
            
            # Grab just the relevant states data
            df = df[df.STATE.isin(fips_int)]
            
            # Find and remove any empty rows
            # sum_cols = list(set(df.columns) - set(xwalk_final.columns))            
            sum_cols = df.select_dtypes(include='number').columns
            df = df[df[sum_cols].sum(axis=1) != 0]
            
            # Find mismatches using symmetric difference of sets
            bad_geos = [str(x) for x in set(df[geo]) - set(xwalk_final[geo])]

            # Remove any missing geographies
            # removed = ', '.join(bad_geos)
            
            print(f'Removing {len(bad_geos)} {geo} from crosswalk with no data or irrelevant data...')
            # print(f'Removed: {removed}')
            
            # xwalk_final = xwalk_final[is_in]
            xwalk_final = xwalk_final[~xwalk_final[geo].isin(bad_geos)]            
            
            # Check for duplicates -- There can be only one control geography per seed PUMA
            if geo.lower() == 'puma':
                continue
            control_counts = xwalk_final[['PUMA', geo]].drop_duplicates().groupby(geo).size()
            dupes = control_counts[control_counts > 1]
            
            # If there are duplicates, choose the zone it is closest to.
            if dupes.shape[0] > 0:
                print(f'Found {dupes.shape[0]} {geo} with multiple PUMAs! De-duping by distance...')
                for zone_id in dupes.index:                                     
                    zone = centroids[geo.lower()].loc[str(zone_id).zfill(11)]
                    
                    # Make sure it's matching within the same State at least!
                    puma_geoms = geoms['puma'][geoms['puma'].STATEFP == zone.STATEFP].geometry
                    
                    # Find distances and nearest puma
                    distances = zone.geometry.distance(puma_geoms)
                    nearest_puma = distances.idxmin()
                    nearest_state = geoms['puma'].loc[nearest_puma].STATEFP
                    
                    assert zone.STATEFP == nearest_state, 'Nearest PUMA is not in the same state!'                    

                    # Approximate degrees to meters conversion
                    if distances[nearest_puma]*111139 < 1000:
                        print(f'Found a PUMA within {distances[nearest_puma]*111139}m for {geo} {zone_id}! Setting to PUMA {nearest_puma}...')
                        xwalk_final.loc[(xwalk_final[geo] == zone_id)]
                        xwalk_final.loc[(xwalk_final[geo] == zone_id), 'PUMA'] = int(nearest_puma)
                    else:
                        print(f'Could not find a PUMA within 1km for {zone_id}! Dropping...')
                        drop_idx = xwalk_final.loc[(xwalk_final[geo] == zone_id) & (xwalk_final.PUMA != nearest_puma)].index
                        xwalk_final.drop(drop_idx, inplace=True)         
        
            # Add to list
            # xwalk_ls.append(xwalk_final)
        
        # Concatenate all states
        # self.XWALK_FINAL = pd.concat(xwalk_ls, ignore_index=True)
        self.XWALK_FINAL = xwalk_final
        
        self.XWALK_FINAL.groupby(['PUMA','STATE']).size()
        
        return

    def save_inputs(self):        
        
        if len(self.PUMS_DATA_FINAL) > 0 and not self.skip_pums:
            for level, path in self.PUMS_DATA_PATHS.items():
                print(f'Saving {level} PUMS data...')
                self.PUMS_DATA_FINAL[level].to_csv(path, index=True)

        if len(self.ACS_DATA_FINAL) > 0 and not self.skip_acs:
            for geo, path in self.ACS_DATA_PATHS.items():
                print(f'Saving {geo} ACS data...')
                self.ACS_DATA_FINAL[geo].to_csv(path, index=False)
            
        if not self.XWALK_FINAL.empty and not self.skip_xwalk:
            print('Saving crosswalk...')
            self.XWALK_FINAL.to_csv(self.XWALK_PATH, index=False)
