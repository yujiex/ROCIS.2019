import glob
import os
import pandas as pd
import shutil
import re

dylos_home_list = ['BHW', 'KD', 'DHP', 'LBB', 'RIR', 'BXJ', 'DCE']
speck_home_list = ['DHP', 'LBB', 'RIR', 'BXJ', 'DCE']
co2_home_list = ['DHP', 'LBB', 'RIR', 'BXJ', 'DCE']

id_mapping = pd.read_csv(os.getcwd() + '/input/id/ROCIS_currentCodeLookup_04-28-17.csv')
id_mapping = id_mapping[['homeID', 'decoderRing']]
replace_dict = dict(zip(id_mapping['homeID'],
                        id_mapping['decoderRing']))

def parent_dir(dirname):
    return dirname[:dirname.find('/routines')]

dylos_merge_path = parent_dir(os.getcwd()) + '/DataBySensor/Dylos/merge_gen_spe/round_all/'
speck_path = parent_dir(os.getcwd()) + '/DataBySensor/Speck/raw_data/round_all_bulkdownload/'

def match_prefix_home_id(filename, home_id):
    return '/' + home_id in filename

def get_dylos_file(home_list):
    filelist = glob.glob(dylos_merge_path + '*.csv')
    outputset = set([x for x in filelist for y in dylos_home_list if match_prefix_home_id(x, y)])
    for z in outputset:
        pattern = re.compile('|'.join(replace_dict.keys()))
        outfile = pattern.sub(lambda m: replace_dict[m.group()], z)
        shutil.copyfile(z, outfile.replace('Dylos/merge_gen_spe/round_all/', 'share_data/Dylos/'))

def get_speck_file(home_list):
    summary = speck_path + 'summary/particle_count_round_all_bulkdownload.csv'
    df_summary = pd.read_csv(summary)
    df_summary = df_summary[df_summary['home_id_standard'].map(lambda x: x in home_list)]
    filelist = df_summary['filename'].tolist()
    for z in filelist:
        filepath = speck_path + z
        pattern = re.compile('|'.join(replace_dict.keys()))
        outfile = re.sub(pattern, lambda m: replace_dict[m.group()], filepath, count=1)
        df = pd.read_csv(filepath)
        df['Date/Time'] = pd.to_datetime(df['EpochTime'], unit = 's')
        cols = list(df)
        df = df[['Date/Time'] + cols[:-1]]
        df.rename(columns=lambda x: x if not '.' in x else x[x.rfind('.') + 1:], inplace=True)
        df.to_csv(outfile.replace('Speck/raw_data/round_all_bulkdownload/', 'share_data/Speck/'), index=False)

get_dylos_file(dylos_home_list)
get_speck_file(speck_home_list)
print 'end'
