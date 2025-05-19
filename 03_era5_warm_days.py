#!/usr/bin/env python
# coding: utf-8

import os
import xarray as xr
import numpy as np
import pandas as pd
import sys

root_dir = '/dartfs-hpc/rc/lab/C/CMIG'
project_dir = os.path.join(root_dir,'agottlieb','snow_nonlinearity')
data_dir = os.path.join(root_dir,'Data','Observations')

era5_dir = os.path.join(root_dir,'Data','Observations','ERA5')
daily_dir = os.path.join(era5_dir,'daily')

mask = xr.open_dataset(os.path.join(era5_dir,'land_mask.nc'))

out_dir = os.path.join(project_dir,'data','interim','era5_tmean_stats')
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
  

y = sys.argv[1]
era5_file = os.path.join(daily_dir,f'tasmean_{y}.nc')
# if os.path.exists(os.path.join(out_dir,f'{y}.nc')):
#     exit()
    
def preprocess(ds):
    ds = ds.rename({list(ds.data_vars)[0]:"t2m"})
    if ds['t2m'].min()>0:
        ds['t2m'] = ds['t2m']-273.15
    return ds

ds = xr.open_dataset(era5_file)
ds = ds.rename({list(ds.data_vars)[0]:"t2m"})
ds = ds.where(mask['lsm']>0.5)
ds = ds.rename({"latitude":"lat","longitude":"lon"})
ds = ds.sel(lat=slice(90,0.25))
ds['lon'] = (ds['lon'] + 180) % 360 - 180
ds = ds.sortby("lon")
ds = ds.sortby("lat")
if ds['t2m'].min()>0:
    ds['t2m'] = ds['t2m']-273.15
    
tavg_mon = ds['t2m'].resample(time='1M').mean()
tavg_mon.name = 'tavg'

std_mon = ds['t2m'].resample(time='1M').std()
std_mon.name = 'tavg_std'

warm_days = ds['t2m'].where(ds['t2m']>0).resample(time='1M').count()
warm_days.name = 'warm_days'

warm_days_plus1 = ds['t2m'].where(ds['t2m']+1>0).resample(time='1M').count()
warm_days_plus1.name = 'warm_days_plus1'

warm_days_plus2 = ds['t2m'].where(ds['t2m']+2>0).resample(time='1M').count()
warm_days_plus2.name = 'warm_days_plus2'
degree_days = ds['t2m'].where(ds['t2m']>0).resample(time='1M').sum()
degree_days = degree_days.where(warm_days.notnull())
degree_days.name = 'degree_days'

merged = xr.merge([tavg_mon,std_mon,warm_days,warm_days_plus1,warm_days_plus2,degree_days])

# merged = xr.merge([tavg_mon,std_mon,warm_days,degree_days])
merged.to_netcdf(os.path.join(out_dir,f"{y}.nc"))