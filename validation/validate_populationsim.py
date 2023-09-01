import pandas as pd
import yaml
import numpy as np
import os
import re
import seaborn as sns
import matplotlib.pyplot as plt


# TODO convert from ValidatePopulationsim.R?



# General functions
def calc_prmse(control: pd.Series, synth: pd.Series) -> float:    
    prmse = (((sum((control - synth)**2) / (sum(control > 0) - 1))**0.5) / sum(control)) * sum(synth > 0) * 100    
    return prmse

# Function to calculate RMSE
def calc_rmse(actual, expected, na_rm = True):
    actual = np.array(actual)
    expected = np.array(expected)
    if na_rm:
        nas = np.logical_or(np.isnan(actual), np.isnan(expected))
        actual = actual[~nas]        
        expected = expected[~nas]
    return np.sqrt(np.mean((actual - expected)**2))

# Create function to calculate normalized rmse
def calc_nrmse(actual, expected):
    exp_avg = np.mean(expected)
    nrmse = calc_rmse(actual, expected, na_rm = True)
    return nrmse / exp_avg
    
def calc_myrmse(exp, avg_exp):    
    avg_exp = [avg_exp] * np.size(exp)
    nrmse = calc_rmse(exp, avg_exp, na_rm = True)
    return nrmse


class Validation:
    
    summaries = {}
    config_dir = os.path.join(os.path.dirname(__file__), 'validation_configs.yaml')
    
    def __init__(self, config_dir) -> None:        
        self.config_dir = config_dir
        
        
    def read_settings(self) -> None:
        print('Reading settings...')
        
        # Read yaml file
        config_path = os.path.join(self.config_dir, 'validation_configs.yaml')
        
        assert os.path.exists(config_path), f'Config file not found: {config_path}'
        
        with open(config_path) as file:
            self.settings = yaml.load(file, Loader=yaml.FullLoader)

        if self.settings['PLOT_GEOGRAPHIES'] is None:
            self.settings['PLOT_GEOGRAPHIES'] = []
        else:
            self.settings.get('PLOT_GEOGRAPHIES', [])

        return
            
            
    def read_data(self) -> None:        
        # Get summaries
        for f in os.listdir(self.settings['OUTPUT_DIR']):
            if 'final_summary' in f:
                print(f'Reading summary: {f}...')
                fpath = os.path.join(self.settings['OUTPUT_DIR'], f)                
                geo = re.findall("summary_(.*?).csv", f)[0]
                df = pd.read_csv(fpath)
                self.summaries[geo] = df
                        
        # Read control files
        print('Reading control files...')
        control_path = [os.path.join(x, y) for x in self.settings['CONFIG_DIRS'] for y in os.listdir(x) if 'controls' in y][0]        
        self.controls = pd.read_csv(control_path)
                
        # Read seed_households
        print('Reading seed_households...')
        hhseed_path = [x for x in os.listdir(self.settings['DATA_DIR']) if 'seed_households' in x][0]
        hhseed_path = os.path.join(self.settings['DATA_DIR'], hhseed_path)
        self.seed_households = pd.read_csv(hhseed_path)
        
        # Read expanded_household_ids
        print('Reading expanded_household_ids...')
        expanded_hhid_path = [x for x in os.listdir(self.settings['OUTPUT_DIR']) if 'expanded_household_ids' in x][0]
        expanded_hhid_path = os.path.join(self.settings['OUTPUT_DIR'], expanded_hhid_path)
        self.expanded_hhid = pd.read_csv(expanded_hhid_path)
                
        return 
        
    
    def process_control(self, control_map: pd.Series) -> pd.Series:
        print(f'Processing control: {control_map["control_field"]}...')

        geography = control_map['geography']
        control_id = control_map['target'] + "_control"
        summary_id = control_map['target'] + "_result"
        control_name = control_map['control_field']
        
        # Fetching data for the geography
        sub_summary = self.summaries[geography]
                
        # Fetching control and synthesized columns
        controls = sub_summary[['id', control_id]]
        synthesized = sub_summary[['id', summary_id]]

        # Renaming columns
        controls.columns = ['GEOGRAPHY', 'CONTROL']   
        synthesized.columns = ["GEOGRAPHY", "SYNTHESIZED"]
        
        # Meta controls are grouped by PUMAs, aggregation is required
        if geography == self.settings['GEOGRAPHIES'][0]:
            controls = controls.groupby('GEOGRAPHY').sum().reset_index()
            synthesized = synthesized.groupby('GEOGRAPHY').sum().reset_index()
                    
        #Fetch and process each control for getting convergance statistics
        compare_data = pd.merge(controls, synthesized, on = 'GEOGRAPHY', how = 'left')
        compare_data = compare_data.astype({'CONTROL': 'float64', 'SYNTHESIZED': 'float64'})
        compare_data[['CONTROL', 'SYNTHESIZED']] = compare_data[['CONTROL', 'SYNTHESIZED']].fillna(0)
        compare_data['DIFFERENCE'] = compare_data['SYNTHESIZED'] - compare_data['CONTROL']
        compare_data['pcDIFFERENCE'] = np.where(compare_data['CONTROL'] > 0, (compare_data['DIFFERENCE'] / compare_data['CONTROL']) * 100, np.nan)
        
        # Calculate statistics
        observed = sum(compare_data['CONTROL'])
        predicted = sum(compare_data['SYNTHESIZED'])
        difference = predicted - observed
        pct_difference = (difference / observed) * 100
        N = sum(compare_data['CONTROL'] > 0)
        prmse = calc_prmse(compare_data['CONTROL'], compare_data['SYNTHESIZED'])
        mean_pct_diff = np.mean(compare_data['pcDIFFERENCE'])
        sdev = np.std(compare_data['pcDIFFERENCE'], ddof = 1)       
        stat_data = pd.Series(
            [control_name, geography, observed, predicted, difference, pct_difference, N, prmse, mean_pct_diff, sdev],
            index=['control_name', 'geography', 'observed', 'predicted', 'difference', 'pct_difference', 'N', 'prmse', 'mean_pct_diff', 'sdev'])
    
        # Preparing data for difference frequency plot
        freq_plot_data = compare_data[compare_data['CONTROL'] > 0].groupby('DIFFERENCE').size().reset_index(name='FREQUENCY')
        
        if geography in self.settings['PLOT_GEOGRAPHIES']:
            # computing plotting parameters            
            xaxis_limit = np.max(np.abs(freq_plot_data['DIFFERENCE'])) + 10
            ylimit = freq_plot_data.FREQUENCY.max()
            
            plot_title = "Frequency Plot: Syn - Control totals for " + control_name
            
            # Frequency Plot
            ax = sns.scatterplot(x = 'DIFFERENCE', y = 'FREQUENCY', data = freq_plot_data, color = 'coral')
            # sns.vlines(x=0, ymin=0, ymax=1, color="red")
            ax.axvline(0, 0, ylimit)
            ax.set_xlim([-xaxis_limit, xaxis_limit])
            ax.set_ylim([0, ylimit])
            ax.set(xlabel='Difference', ylabel='Frequency', title=plot_title)            
            ax.figure.tight_layout()
            # plt.show()
            fname = os.path.join(self.settings['VALID_DIR'], 'plots', control_id + ".png")        
            ax.figure.savefig(fname)
            plt.close()
            
        return stat_data


    def run_validation(self) -> None:
        print('Running validation...')
        
        self.read_settings()
        self.read_data()
                
        #Create plot directory
        if not os.path.exists(os.path.join(self.settings['VALID_DIR'], 'plots')):
            os.makedirs(os.path.join(self.settings['VALID_DIR'], 'plots'))

        # Computing convergance statistics and write out results            
        assert self.controls is not None, "Controls not found"
        stats = self.controls.apply(self.process_control, axis=1)
        
        fname = os.path.join(self.settings['VALID_DIR'], 'populationsim_stats.csv')
        stats.to_csv(fname, index=False)

        
        # #for fresno only - set PRMSE and sdev to 0 as there is only one region
        # stats[stats$geography == 'REGION', ]$prmse <- 0 
        # stats[stats$geography == 'REGION', ]$sdev <- 0

        # Convergance plot
        print('Generating SDEV convergence plot')
        ax = sns.scatterplot(y = 'control_name', x = 'mean_pct_diff', data = stats, color = 'steelblue')
        ax.errorbar(y = 'control_name', x = 'mean_pct_diff', data = stats, xerr = 'sdev', fmt = 'o', color = 'steelblue')
        ax.axvline(0, 0, 1, color="coral")
        ax.set(ylabel='Control', xlabel='Percentage Difference', title='Region PopulationSim Controls Validation (Mean +/- SDEV)')    
        ax.figure.set_size_inches(8, 10)
        ax.figure.tight_layout()
        # plt.show()        
        ax.figure.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', 'convergance-sdev.png'))
        plt.close()
        
        # Convergance plot
        print('Generating PRMSE convergence plot')
        ax = sns.scatterplot(y = 'control_name', x = 'mean_pct_diff', data = stats, color = 'steelblue')
        ax.errorbar(y = 'control_name', x = 'mean_pct_diff', data = stats, xerr = 'prmse', fmt = 'o', color = 'steelblue')
        ax.axvline(0, 0, 1, color="coral")
        ax.set(ylabel='Control', xlabel='Percentage Difference', title='Region PopulationSim Controls Validation (Mean +/- PRMSE)')
        ax.figure.set_size_inches(8, 10)
        ax.figure.tight_layout()
        ax.figure.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', 'convergance-prmse.png'))
        plt.close()
        
        # Uniformity Analysis
        plot_geos = self.settings['PLOT_GEOGRAPHIES']
        uniformity_geos = plot_geos[:(plot_geos.index(self.settings['SEED_GEOGRAPHY']) + 1)]
        
        for geo in uniformity_geos:
            print(f'Generating uniformity analysis plot for {geo}...')
            self.expanded_hhid['FINALWEIGHT'] = 1
            summary_hhid = self.expanded_hhid[['hh_id', 'FINALWEIGHT']].groupby('hh_id').sum().reset_index()
            uniformity = pd.merge(self.seed_households[['hh_id', 'WGTP', geo]], summary_hhid, on = 'hh_id', how = 'left')        
            uniformity['FINALWEIGHT'].fillna(0, inplace = True)                
            uniformity['EXPANSIONFACTOR'] = uniformity['FINALWEIGHT'] / uniformity['WGTP']        
            
            # Plotting EF distribution by PUMA
            ax = sns.histplot(data = uniformity, x = 'EXPANSIONFACTOR', hue = geo, binwidth=0.5, multiple = 'dodge', palette = 'Set2')
            ax.set(xlabel='Expansion Factor', ylabel='Count', title=f'Expansion Factor Distribution by {geo}')
            ax.figure.set_size_inches(15, 10)
            ax.figure.tight_layout()
            plt.yscale('log')            
            ax.figure.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', f'EF-Distribution_{geo}.png'))
            plt.close()
        
            # Uniformity Analysis            
            u_analysis_geo = uniformity.groupby(geo).apply(lambda x: pd.Series({
                'W': x['WGTP'].sum(),
                'Z': x['FINALWEIGHT'].sum(),
                'N': x['hh_id'].count(),
                'EXP': x['WGTP'].sum() / x['FINALWEIGHT'].sum(),
                'EXP_MIN': x['EXPANSIONFACTOR'].min(),
                'EXP_MAX': x['EXPANSIONFACTOR'].max(),
                'NRMSE': calc_myrmse(x['EXPANSIONFACTOR'], x['WGTP'].sum() / x['FINALWEIGHT'].sum()),
            })).reset_index()
                        
            # Plotting NRMSE by PUMA
            fname = os.path.join(self.settings['VALID_DIR'], f'NRMSE_{geo}.csv')
            u_analysis_geo.to_csv(fname, index=False)


if __name__ == '__main__':
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'populationsim/configs')
    
    v = Validation(config_dir)
    v.run_validation()