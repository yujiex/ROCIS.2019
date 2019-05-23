import pandas as pd
import os
import glob

import util

dirname = '/media/yujiex/work/ROCIS/ROCIS/DataBySensor/CO2/combine/reformat/'

def concat(gb_list,cohort=None, history=True, home=None):
    print 'concatenating dylos files by home id standard ...'
    df_summary = pd.read_csv(dirname + 'summary/field2.csv')

    if not cohort is None:
        if not history:
            df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
            ids = df_id[df_id['ROUND'] == cohort]['HOME ID CORRECT']
        else:
            df_id = pd.read_csv(os.getcwd() + '/input/participant_cohort_ongoing.csv')
            ids = df_id[df_id['cohort'] == cohort]['home_id_standard']
        df_summary = df_summary[df_summary['home_id_standard'].isin(ids)]
    if not home is None:
        df_summary = df_summary[df_summary['home_id_standard'] == home]
    gr = df_summary.groupby(['home_id_standard'] + gb_list)
    suf = '_'.join(map(lambda x: x[:3], gb_list))
    path = dirname + 'concat_gen_spe/'
    errorfiles = ['BHW_O_D114_04-02-17_WITH ERRORS.txt']
    for name, group in (list(gr)):
        print 'write to home: {0}'.format(name)
        files = group['filename'].tolist()
        files = [f for f in files if f not in errorfiles]
        dfs = []
        for f in files:
            print f
            df = pd.read_csv(dirname + f)
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=False)
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
        df = df_all.sort_values(by=['timestamp'])
        df.drop_duplicates(subset=['timestamp'], inplace=True)
        print path
        df.to_csv('{0}{1}.csv'.format(path,
                                       '_'.join(name)), index=False)
    return

def combine_dygraph_IOR(loc, cohort=None):
    # id_hash()
    files = glob.glob(dirname + 'concat_gen_spe/*.csv')
    print len(files)
    df_id = pd.read_csv(os.getcwd() + '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
    df_time = pd.read_csv(os.getcwd() + '/input/cohort_time.csv')
    df_time.set_index('round', inplace=True)
    df_hash = pd.read_csv(os.getcwd() + '/input/id_hashing.csv')
    df_hash.set_index('home_id_standard', inplace=True)
    nb_dict = dict(zip(df_id['HOME ID CORRECT'],
                       df_id['NEIGHBORHOOD']))
    if not cohort is None:
        ids = df_id[df_id['ROUND'] == cohort]['HOME ID CORRECT']
        cutoff_start = df_time.ix[cohort, 'cohort start time']
        cutoff_end = df_time.ix[cohort, 'cohort end time']
    else:
        ids = df_id['HOME ID CORRECT']
    print len(ids)
    outfiles = []
    for name in ids:
        print '{0}_{1}'.format(name, loc)
        outfiles += [x for x in files if '{0}_{1}'.format(name, loc)
                     in x]
    print len(outfiles)
    df_id.set_index('HOME ID CORRECT', inplace=True)
    dfs = []
    # outfiles = [x for x in outfiles if 'CAW' in x]
    for f in outfiles:
        print f
        df = pd.read_csv(f)
        filename = f[f.rfind('/') + 1:]
        print filename
        name = filename[:filename.find('_')]
        print name
        loc = filename[filename.find('_') + 1: filename.rfind('.')]
        hashing = df_hash.ix[name, 'hashing']
        nb = nb_dict[name]
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['timestamp'])), inplace=True)
        print len(df.index)
        if 'cutoff_start' in locals():
            df = df[df.index > pd.to_datetime(cutoff_start)]
        if 'cutoff_end' in locals():
            df = df[df.index < pd.to_datetime(cutoff_end)]
        if len(df) == 0:
            continue
        df_r = df.resample('15T', how='mean')
        newname = '{0}_{1}_{2}'.format(hashing, loc, nb)
        df_r.rename(columns={'field2': newname}, inplace=True)
        dfs.append(df_r[[newname]])
    print len(dfs)
    df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), dfs)
    if cohort is None:
        df.to_csv('{0}across/round_all/co2_all.csv'.format(dirname))
    else:
        df.to_csv('{0}across/round_{1}/co2_{1}.csv'.format(dirname, cohort))
    with open(util.get_path('Dylos', 'across', 'all') + 'template.html', 'r') as rd:
        lines = rd.readlines()
    length = len(lines)
    for i in range(length):
        lines[i] = lines[i].replace("AE_Small_15T.csv", "co2_{0}.csv".format(cohort))
        if not cohort is None:
            lines[i] = lines[i].replace("AE_Small 15 Min Average", "Cohort {0} CO2 (ppm)".format(cohort))
        else:
            lines[i] = lines[i].replace("AE_Small 15 Min Average", "Cohort {0} CO2 (ppm)")
    if cohort is None:
        with open ('{}across/round_all/co2.html'.format(dirname), 'w+') as wt:
            wt.write(''.join(lines))
    else:
        with open ('{}across/round_{}/co2.html'.format(dirname, cohort), 'w+') as wt:
            wt.write(''.join(lines))

home = None
cohort = 20
# concat(['general_location_standard', 'specific_location_standard'],
#        cohort=cohort, history=False, home=home)

combine_dygraph_IOR('O', cohort=cohort)
