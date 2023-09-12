import pandas as pd
import yaml
import numpy as np
import os
import re
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
tqdm.pandas()


# General functions
def calc_prmse(control: pd.Series, synth: pd.Series) -> float:    
    prmse = 0
    if control.shape[0] > 1:    
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
        
    
    def process_control(self, control_map: pd.Series, plot: bool = True, quantile: str|None = None) -> pd.Series:
        
        geography = control_map['geography']
        control_id = control_map['target'] + "_control"
        summary_id = control_map['target'] + "_result"
        control_name = control_map['control_field']
        
        # print(f'Processing control {geography}: {control_name}...')
        
        # Fetching data for the geography
        sub_summary = self.summaries[geography]
        
        if 'geo_quantiles' in sub_summary.columns and quantile is not None:
            sub_summary = sub_summary[sub_summary['geo_quantiles'] == quantile]
                
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
        
        # Combine result
        stat_data = pd.Series(
            [control_name, geography, observed, predicted, difference, pct_difference, N, prmse, mean_pct_diff, sdev],
            index=['control_name', 'geography', 'observed', 'predicted', 'difference', 'pct_difference', 'N', 'prmse', 'mean_pct_diff', 'sdev'])

        if 'geo_quantiles' in sub_summary.columns and quantile is not None:
            stat_data['geo_quantiles'] = quantile

        # Frequency Plot    
        if plot:
            # Preparing data for difference frequency plot
            freq_plot_data = compare_data[compare_data['CONTROL'] > 0].groupby('DIFFERENCE').size().reset_index(name='FREQUENCY')
            self.plot_frequencies(freq_plot_data, control_id, geography, control_name)

            
        return stat_data


    def run_validation(self) -> None:
        print('Running validation...')
        
        self.read_settings()
        self.read_data()
                
        #Create plot directory
        if not os.path.exists(os.path.join(self.settings['VALID_DIR'], 'plots/controls')):
            os.makedirs(os.path.join(self.settings['VALID_DIR'], 'plots/controls'), exist_ok=True)
            
        # Distribution of BG size        
        print(f'Generating geography size distribution plots')
        for geo in self.settings['SUB_GEOGRAPHIES']:            
            self.summaries[geo]['geo_quantiles'] = pd.qcut(
                self.summaries[geo]['h_total_control'], 
                q=4, 
                labels=['Q1', 'Q2', 'Q3', 'Q4']
            )
            
            # plot geo size distribution
            self.plot_geo_distribution(self.summaries, geo)
        
        # Computing convergance statistics and write out results            
        assert self.controls is not None, "Controls not found"
        
        super_geos = set(self.settings['GEOGRAPHIES']) - set(self.settings['SUB_GEOGRAPHIES'])        
        control_geos = list(super_geos)
        control_geos.extend([None])        
        for geo in control_geos:
            agg_controls = self.controls.copy()
            
            if geo is None:
                plot = True
                geo = '-'.join(self.settings.get('SUB_GEOGRAPHIES'))
                
                def calc_qstats(k):
                    return agg_controls.apply(lambda x: self.process_control(x, plot=False, quantile=k), axis=1)

                print(f'Calculating quantile statistics for {geo}')
                qstats_dict = {k: calc_qstats(k) for k in ['Q1', 'Q2', 'Q3', 'Q4']}
                qstats_df = pd.concat(qstats_dict.values())
                fname = os.path.join(self.settings['VALID_DIR'], f'populationsim_stats_quantiles.csv')
                qstats_df.to_csv(fname, index=False)
                
                # Convergance plot
                self.plot_convergence_quantiles(qstats_dict, 'sdev', geo)
                self.plot_convergence_quantiles(qstats_dict, 'prmse', geo)
                                
            else:
                agg_controls['geography'] += f'_{geo}'
                plot = False

            assert hasattr(agg_controls, 'progress_apply')
            
            print(f'Calculating statistics for {geo}')
            stats = agg_controls.progress_apply(lambda x: self.process_control(x, plot=plot, quantile=None), axis=1)
            fname = os.path.join(self.settings['VALID_DIR'], f'populationsim_stats_{geo}.csv')
            
            stats.fillna('', inplace=False).to_csv(fname, index=False)

            # Convergance plots
            self.plot_convergence(stats, 'sdev', geo)
            self.plot_convergence(stats, 'prmse', geo)
        
        # Uniformity Analysis        
        for geo in super_geos:
            self.expanded_hhid['FINALWEIGHT'] = 1
            summary_hhid = self.expanded_hhid[['hh_id', 'FINALWEIGHT']].groupby('hh_id').sum().reset_index()
            uniformity = pd.merge(self.seed_households[['hh_id', 'WGTP', geo]], summary_hhid, on = 'hh_id', how = 'left')        
            uniformity['FINALWEIGHT'].fillna(0, inplace = True)                
            uniformity['EXPANSIONFACTOR'] = uniformity['FINALWEIGHT'] / uniformity['WGTP']        
            
            # Plotting EF distribution by PUMA
            self.plot_EF_distribution(uniformity, geo)
        
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

    def plot_convergence(self, stats_df: pd.DataFrame, stat_var: str, geo: str) -> None:
        print(f'Generating {stat_var.upper().upper()} convergence plot for {geo}')
        
        fig, ax = plt.subplots(figsize=(8, 10))
        sns.scatterplot(y = 'control_name', x = 'mean_pct_diff', data = stats_df, color = 'steelblue', s=20, ax=ax)
        
        if not stats_df[stat_var].isna().all():           
            ax.errorbar(y = 'control_name', x = 'mean_pct_diff', data = stats_df,
                        xerr = stat_var, fmt = '+', color = 'steelblue',
                        elinewidth = 0.5, capsize = 2, capthick = 1)
            
        ax.axvline(0, 0, 1, color="coral", linewidth=0.5)
        ax.set(ylabel='Control', xlabel='Percentage Difference', xlim=(-100, 100), 
               title=f'{geo} PopulationSim Controls Validation (Mean +/- {stat_var.upper()})')
        fig.tight_layout()
        fig.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', f'convergance-{stat_var}_{geo}.png'))
        plt.close()
        
        return
    
    def plot_convergence_quantiles(self, qstats_dict: dict, stat_var: str, geo: str) -> None:
        print(f'Generating {stat_var.upper().upper()} convergence plot for {geo} h_total quantiles')
        
        fig, axes = plt.subplots(nrows=1, ncols=4, sharey=True, constrained_layout=True, figsize=(15, 8))                
        fig.suptitle(f'Validation by {geo} h_total Quantile (Mean +/- SDEV)')
        for i, k in enumerate(['Q1', 'Q2', 'Q3', 'Q4']):
            ax = axes[i]
            qstats = qstats_dict[k]
            sns.scatterplot(
                y = 'control_name', x = 'mean_pct_diff', data = qstats,
                color = 'steelblue', size=3, ax=ax, legend=False
            )                    
            ax.errorbar(y = 'control_name', x = 'mean_pct_diff', data = qstats,
                        xerr = stat_var, fmt = '+', color = 'steelblue',
                        elinewidth = 0.5, capsize = 2, capthick = 1)
            ax.axvline(0, 0, 1, color="coral", linewidth=0.5)
            ax.set(ylabel='Control', xlabel=f'{k} Percentage Difference', xlim=(-100, 100))
            ax.set_xticks(ax.get_xticks())
            ax.set_xticklabels(ax.get_xticklabels(), fontsize=6)
        axes[0].set_yticks(axes[0].get_yticks())
        axes[0].set_yticklabels(axes[0].get_yticklabels(), fontsize=6)
        # fig.tight_layout()
        fig.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', f'convergance-{stat_var}_{geo}_quantiles.png'))
        plt.close()
        
        return

    def plot_EF_distribution(self, uniformity: pd.DataFrame, geo: str) -> None:
        print(f'Generating expansion factor distribution uniformity analysis plot for {geo}')
        fig, ax = plt.subplots(figsize=(15, 10))                 
        sns.histplot(data = uniformity, x = 'EXPANSIONFACTOR', hue = geo, binwidth=0.5, multiple = 'dodge', palette = 'Set2', ax=ax)
        ax.set(xlabel='Expansion Factor', ylabel='Count', title=f'Expansion Factor Distribution by {geo}')
        fig.tight_layout()
        plt.yscale('log')            
        plt.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', f'EF-Distribution_{geo}.png'))
        plt.close()
        
        return

    def plot_geo_distribution(self, summaries_dict: dict, geo: str) -> None:
        print(f'Generating {geo} size distribution plot')        
        fig, ax = plt.subplots(figsize=(15, 10))
        sns.histplot(data = summaries_dict[geo], x = 'h_total_control', binwidth=100, multiple = 'dodge', ax=ax)
        ax.set(ylabel='Frequency', xlabel='Households', title=f'Distribution of {geo} sizes')
        fig.set_size_inches(8, 6)
        fig.tight_layout()
        fig.savefig(os.path.join(self.settings['VALID_DIR'], 'plots', f'geo-h_total-distribution_{geo}.png'))
        plt.close()
        
        return

    def plot_frequencies(self, freq_plot_data: pd.DataFrame, control_id: str, geography: str, control_name: str) -> None:
        
        # print(f'Generating frequency plot for {geography}: {control_name}')
        
        # computing plotting parameters            
        xaxis_limit = np.max(np.abs(freq_plot_data['DIFFERENCE'])) + 10
        ylimit = freq_plot_data.FREQUENCY.max()
        
        plot_title = f"Frequency Plot: Syn - Control totals for {control_name} in {geography}"

        fig, ax = plt.subplots(figsize=(15, 10))
        sns.scatterplot(x = 'DIFFERENCE', y = 'FREQUENCY', data = freq_plot_data, color = 'coral', s=20, ax=ax)
        ax.axvline(0, 0, ylimit, color="steelblue", linewidth=0.5)
        ax.set(xlabel='Difference', ylabel='Frequency', title=plot_title, 
               xlim=(-xaxis_limit, xaxis_limit), ylim=(0, ylimit))
        fig.tight_layout()
        fname = os.path.join(self.settings['VALID_DIR'], 'plots/controls', f'{control_id}_{geography}.png')        
        plt.savefig(fname)
        plt.close()


if __name__ == '__main__':
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'populationsim/configs')
    
    v = Validation(config_dir)
    v.run_validation()