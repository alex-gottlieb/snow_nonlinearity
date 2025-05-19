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

ghcn_dir = os.path.join(project_dir,'data','raw','ghcnd')
ghcn_files = [os.path.join(ghcn_dir,f) for f in os.listdir(ghcn_dir)]

def to_xarray(f):
    try:
        df = pd.read_csv(f,header=None).iloc[:,:4]
        df.columns = ['site','date','var','val']
        df = df[df['var'].isin(['SNWD','TMAX','TMIN','TAVG','PRCP'])]
        df['date'] = pd.to_datetime(df['date'],format='%Y%m%d')
        df_long = df.pivot(index=['site','date'],columns='var',values='val')
        if 'TAVG' not in df_long.columns:
            df_long['TAVG'] = (df_long['TMAX']+df_long['TMIN'])/2
        df_long['TAVG'] = df_long['TAVG']/10
        df_long['TMAX'] = df_long['TMAX']/10
        df_long['TMIN'] = df_long['TMIN']/10
        df_long = df_long.rename(columns={"TAVG":"tavg","TMAX":"tmax","TMIN":"tmin","SNWD":"sd","PRCP":"ppt"})
        da = xr.Dataset.from_dataframe(df_long[['tavg','tmax','tmin','sd','ppt']])
        return da
    except:
        return None
    
pool = Pool(8)
res = pool.map(to_xarray,ghcn_files)
res = [r for r in res if r is not None]
res = xr.concat(res,dim='site')
res = res.rename({"date":"time"})
if os.path.exists(os.path.join(project_dir,'data','interim','ghcnd.nc')):
    os.remove(os.path.join(project_dir,'data','interim','ghcnd.nc'))
res.to_netcdf(os.path.join(project_dir,'data','interim','ghcnd.nc'))
pool.close()