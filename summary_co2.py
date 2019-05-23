import pandas as pd
import glob
import numpy as np
import os
import re

# dirname = '/media/yujiex/work/ROCIS/ROCIS/DataBySensor/CO2/combine/reformat/'
dirname = '/Users/yujiex/Dropbox/ROCIS/DataBySensor/CO2/combine/reformat/'

def tidy_name(name):
    name = name.upper()
    name = name.replace('-', '_')
    name = name.replace('CO2', '')
    name = name.replace('C02', '')
    name = name.replace('__', '_')
    name = name.replace('.TXT', '')
    return name
    
def get_ior(fieldlist):
    if 'I' in fieldlist or 'R' in fieldlist or 'INDOOR' in fieldlist:
        return 'I'
    elif 'O' in fieldlist or 'OUTDOOR' in fieldlist or 'OUTSIDE' in fieldlist:
        return 'O'
    elif 'S' in fieldlist:
        return 'S'
    else:
        return '-'
        
def get_equipid(name):
    prog_equip = re.compile('[0-9]{3,4}')
    if re.search(prog_equip, name):
        return re.search(prog_equip, name).group()
    else:
        return '-'

def get_specific(fieldlist, df_room, specific_loc_upper):
    for x in fieldlist:
        print x
        if x in specific_loc_upper:
            return df_room.ix[x, 'target']
    return '-'
    
def get_tokens(name, floor_list, df_level):
    name = name.replace('.', '_')
    name = name.replace(' ', '_')
    floor = '-'
    for m in floor_list:
        if m in name:
            name = name.replace(m, '_')
            floor = df_level.ix[m, 'target']
            return name.split('_') + [floor]
    return name.split('_') + [floor]

def get_floor(tokens, floor_output_list):
    for t in tokens:
        if t in floor_output_list:
            return t
    return '-'
    
def is_number(x):
    try: 
        int(x)
        return True
    except ValueError:
        return False
    
def standardize_id(name, id_dict):
    if name in id_dict:
        return id_dict[name]
    elif is_number(name):
        return '-'
    elif len(name) < 2 or len(name) > 4:
        return '-'
    return name

def summary_label():
    files = glob.glob(dirname + '*.csv')
    filenames = [f[f.rfind('/') + 1:] for f in files]
    df = pd.DataFrame({'filename': filenames})
    # df = df[df['filename'] == 'CAW_I_CO2-925_07-29-2016_2ndFlMBedroom.txt.csv']
    df['filename_tidy'] = df['filename'].map(tidy_name)
    df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    id_dict = dict(zip(df_id['INITIALS'], df_id['HOME ID CORRECT']))
    df_room = pd.read_csv(os.getcwd() + '/input/room_mapping.csv')
    df_room.set_index('source', inplace=True)
    specific_loc_upper = df_room.index.tolist()
    df_level = pd.read_csv(os.getcwd() + '/input/level_mapping.csv')
    df_level.set_index('source', inplace=True)
    floor_list = df_level.index.tolist()
    floor_output_list = set(df_level['target'].tolist())
    print floor_output_list
    # get rid of floor and room being one token
    df['home_id_standard'] = df['filename_tidy'].map(lambda x: x[:x.find('_')])
    df['home_id_standard'] = df['home_id_standard'].map(lambda x: standardize_id(x, id_dict))
    df['tokens'] = df['filename_tidy'].map(lambda x: get_tokens(x, floor_list, df_level))
    df['general_location_standard'] = df['tokens'].map(get_ior)
    df['floor'] = df['tokens'].map(lambda x: get_floor(x, floor_output_list))
    df['specific_location_standard'] = df['tokens'].map(lambda x: get_specific(x, df_room, specific_loc_upper))
    df['equipment_id_standard'] = df['filename_tidy'].map(get_equipid)
    df_overwriteIOR = pd.read_csv('{}/ior.csv'.format(dirname.replace("reformat", "input")))
    df_overwriteIOR.dropna(axis=0, how='any', inplace=True)
    overwrite_colname = 'ior'
    colname = 'general_location_standard'
    ori_dict = dict(zip(df['filename'], df[colname]))
    overwriteIOR_dict = dict(zip(df_overwriteIOR['filename'],
                                 df_overwriteIOR[overwrite_colname]))
    oridict = ori_dict.update(overwriteIOR_dict)
    df[colname] = df['filename'].map(ori_dict)
    df.drop('tokens', axis=1, inplace=True)
    print df.head()
    df.to_csv(dirname + 'label/label.csv', index=False)
    
def int_describe(df):
    for col in df:
        df[col] = df[col].map(lambda x: x if np.isnan(x) else (int(round(x, 0))))
    return df

def summary_stat():
    filelist = glob.glob(dirname + '*.csv')
    # print (filelist)
    categories = list(pd.read_csv(filelist[0]))
    print categories
    categories.remove('timestamp')
    df_dict = dict(zip(categories, [[] for i in
                                    range(len(categories))]))
    emptys = ['filename\n']
    for (i, f) in enumerate(filelist):
        print i, f
        filename = f[f.rfind('/') + 1:]
        df = pd.read_csv(f)
        if len(df) == 0:
            df_cate = df.copy()
            emptys.append(filename + '\n')
            # df_disc_cate['filename'] = filename
            continue
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        for cate in categories:
            df_cate = df.copy()
            df_cate = df_cate[['timestamp', cate]]
            df_disc_cate = int_describe(df_cate.describe())
            df_disc_cate = df_disc_cate.transpose()
            df_disc_cate['filename'] = filename
            df_disc_cate['Raw Start Time'] = df['timestamp'].min()
            df_disc_cate['Raw End Time'] = df['timestamp'].max()
            df_disc_cate['Raw duration/[min]'] = \
                    (df_disc_cate['Raw End Time'] - \
                     df_disc_cate['Raw Start Time']) / np.timedelta64(1,'m') + 1
            df_disc_cate['missing data count'] = \
                    (df_disc_cate['Raw duration/[min]'] // 15 + 1) - df_disc_cate['count']
            df_dict[cate].append(df_disc_cate)

    for cate in categories:
        df_all = pd.concat(df_dict[cate], ignore_index=True)
        df_all.drop_duplicates(subset=['filename'], inplace=True)
        df_all.to_csv(dirname + \
                      'stat_summary/{0}.csv'.format(cate),
                      index=False)
        
def summary_all_general():
    df_label = pd.read_csv(dirname + 'label/label.csv')
    files_stat = glob.glob(dirname + 'stat_summary/*.csv')
    for f in files_stat:
        filename = f[f.rfind('/') + 1:]
        datecols = ['Raw Start Time', 'Raw End Time']
        df_stat = pd.read_csv(f, parse_dates=datecols)
        # keep non-empty files
        df_1 = pd.merge(df_stat, df_label, on='filename', how='inner')
        cols = set(list(df_1))
        head = ['home_id_standard',
                'filename', 'specific_location_standard', 'equipment_id_standard', 'floor', 'general_location_standard', 'Raw Start Time', 'Raw End Time']
        tail = cols.difference(set(head))
        newcols = head + list(tail)
        df_1 = df_1[newcols]
        df_1.sort_values(by=['home_id_standard', 'Raw End Time', 'general_location_standard'], inplace=True)
        df_1.to_csv(dirname + 'summary/{0}'.format(filename), index=False)
    print 'summary_all_gen'

# remove duplicate files in the summary folder by statistic and home ID
def remove_dup_files():
    gb_factor = ['home_id_standard', '50%', 'max', 'std', 'Raw Start Time', 'Raw End Time']
    for i in range(1, 4):
        f = dirname + 'summary/field{}.csv'.format(i)
        df = pd.read_csv(f)
        print
        print len(df)
        df.sort_values(by=['Raw Start Time', 'Raw End Time', 'general_location_standard'], inplace=True)
        df3 = df.groupby(gb_factor).last()
        print len(df3)
        df3.reset_index(inplace=True)
        cols = set(list(df3))
        print(cols)
        head = ['specific_location_standard','home_id_standard',
                'filename',
                'general_location_standard',
                'equipment_id_standard', 'Raw Start Time', 'Raw End Time']
        tail = cols.difference(set(head))
        newcols = head + list(tail)
        df3 = df3[newcols]
        df3.sort_values(by=['home_id_standard', 'Raw End Time', 'general_location_standard'], inplace=True)
        df3.to_csv(f.replace('.csv', '_unique.csv'), index=False)
    print 'end'
    return
                                                                    
summary_label()
summary_stat()
summary_all_general()
remove_dup_files()
