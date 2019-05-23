import re
import glob
import util
import pandas as pd

dirname = '/Users/yujiex/Dropbox/ROCIS/DataBySensor/CO2/combine/'
# dirname = '/media/yujiex/work/ROCIS/ROCIS/DataBySensor/CO2/combine/'

def format_match(string):
    pattern = re.compile('^\s+([0-9]{1,4})  ([0-9]{2}-){2}[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}\s+[0-9\.]{3,5}\s+([0-9]{1,4})\s+[0-9\.]{3,5}\s*\r\n')
    match = re.match(pattern, string)
    return match != None
    
def clean():
    files = glob.glob(dirname + '*.txt')
    errlines = []
    for i, f in enumerate(files[0:]):
        filename = f[f.rfind('/') + 1:]
        print i, filename
        with open(f, 'r') as rd:
            lines = rd.readlines()
        newlines = [line for line in lines if format_match(line)]
        errlines += [line for line in lines if (not format_match(line))]
        errlines.append(filename + '------------------\r\n')
        clean_file = '{0}clean/{1}'.format(dirname, filename)
        if len(newlines) > 1:
            with open(clean_file, 'w+') as wt:
                wt.write(''.join(newlines))
    with open(dirname + 'removed/summary.txt', 'w') as wt:
        wt.write(''.join(errlines))

def reformat():
    files = glob.glob(dirname + 'clean/*.txt')
    for i, f in enumerate(files):
        filename = f[f.rfind('/') + 1:]
        print i, filename
        df = pd.read_fwf(f, names=['hours', 'field1', 'field2', 'field3'])
        if len(df) == 0:
            print 'empty data'
            continue
        df.reset_index(inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        df['timestamp'] = df.apply(lambda r: '{0}  {1}'.format(r['level_1'], r['hours']), axis=1)
        df = df[['timestamp', 'field1', 'field2', 'field3']]
        df.to_csv(dirname + 'reformat/{0}.csv'.format(filename), index=False)

clean()
reformat()
