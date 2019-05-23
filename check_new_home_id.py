import pandas as pd
import os
import glob
from os.path import expanduser
homepath = expanduser("~")

cohort_num = '38'
folder_suffix = 'all'
# cohort_num = 'intervention'
# folder_suffix = 'intervention'

df = pd.read_csv(os.getcwd() + \
    '/input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv')
exist_ids = df[df['ROUND'] == cohort_num]['HOME ID CORRECT'].tolist()
files = glob.glob(homepath + '/Dropbox/ROCIS/DataBySensor/Dylos/new_data/round_{}/*'.format(folder_suffix))

filenames = [x[x.rfind('/') + 1:] for x in files]
filenames = [x if x[0]!="_" else x[1:] for x in filenames]

new_ids = set([x[:3].upper() for x in filenames])
# print new_ids

print [x for x in new_ids if x not in exist_ids]
