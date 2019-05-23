import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from dateutil import tz
from os.path import expanduser
homepath = expanduser("~")

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

# plot_loc = "outdoor"
# plot_loc = "indoor"
# for plot_loc in ["outdoor"]:
for plot_loc in ["indoor", "outdoor"]:
    files = glob.glob(homepath + '/Dropbox/ROCIS/DataBySensor/PurpleAir/{}_cmp_plots/raw_data/*.csv'.format(plot_loc))
    for f in files:
        print(f)
        df = pd.read_csv(f)
        df['created_at'] = pd.to_datetime(df['created_at'])
        if plot_loc == "indoor":
            df = df[df['created_at'] > pd.to_datetime("2018-05-14")]
        df.set_index(pd.DatetimeIndex(pd.to_datetime(df['created_at'])), inplace=True)
        df_r = df.resample('15T', how='mean')
        df_r.dropna(how='all', subset=list(df_r), inplace=True)
        df_r.to_csv(f.replace('raw_data/', '15min_avg/{}_'.format(plot_loc)))
