#!/usr/bin/env python
# coding: utf-8

import os
import xarray as xr
import numpy as np
import pandas as pd
from multiprocessing import Pool
import warnings
warnings.filterwarnings("ignore")

root_dir = '/dartfs-hpc/rc/lab/C/CMIG'
project_dir = os.path.join(root_dir,'agottlieb','snow_nonlinearity')
snotel_dir = os.path.join(root_dir,'Data','Observations','SNOTEL')
snotel_data_dir = os.path.join(snotel_dir,'station_data',)
snotel_data_files = [os.path.join(snotel_data_dir,f) for f in os.listdir(snotel_data_dir) if f.endswith("csv")]
snotel_data_files.sort()

def to_xarray(f):
    try:
        df = pd.read_csv(f)
        df.columns = df.columns.str.lower()
        df['date'] = pd.to_datetime(df['date'])
        df['swe'] = 25.4*df['swe'] # in to mm
        df['ppt'] = 25.4*df['ppt'] # in to mm
        for col in ['tmax','tmin','tavg']:
            df[col][df[col]>120] = np.nan # mask unrealistic values
            df[col][df[col]<-50] = np.nan # mask unrealistic values
            df[col] = (df[col]-32)*(5/9) # F to C
        da = xr.Dataset.from_dataframe(df.set_index(['date']))
        da = da.assign_coords(site=f.split("/")[-1].split(".")[0])
        return da
    except:
        return None

pool = Pool(8)
res = pool.map(to_xarray,snotel_data_files)
res = [r for r in res if r is not None]
res = xr.concat(res,dim='site')
res = res.rename({"date":"time",}).drop('p_accum')
res.to_netcdf(os.path.join(project_dir,'data','interim','snotel.nc'))
pool.close()
