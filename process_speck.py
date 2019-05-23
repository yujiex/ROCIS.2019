import pandas as pd
import glob
import os
import util

file_path = util.parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_manual_download/'
files = glob.glob(file_path + '*.csv')
for i, f in enumerate(files):
    print i, f
    df = pd.read_csv(f)
    df.rename(columns={'sample_timestamp_unix_time_secs':
                        'EpochTime'}, inplace=True)
    df['Date/Time'] = (pd.to_datetime(df['EpochTime'], unit = 's'))
    df.to_csv(f.replace('round_all_manual_download/', 'round_all_manual_download/convert_time_from_unix/'))
