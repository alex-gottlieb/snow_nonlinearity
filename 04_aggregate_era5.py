#!/usr/bin/env python
# coding: utf-8

import os
import xarray as xr
import xesmf as xe
import numpy as np
import pandas as pd
import sys

root_dir = '/dartfs-hpc/rc/lab/C/CMIG'
project_dir = os.path.join(root_dir,'agottlieb','snow_nonlinearity')
era5_dir = os.path.join(project_dir,'data','interim','era5_tmean_stats')
era5_files = [os.path.join(era5_dir,f) for f in os.listdir(era5_dir)]
era5_files.sort()

ds = xr.concat([xr.open_dataset(f) for f in era5_files],dim='time')
ds_ndjfm = ds.where((ds['time.month']>=11)|(ds['time.month']<=3))
ds_ndjfm_m = ds_ndjfm[['tavg','tavg_std']].resample(time='AS-OCT').mean()
ds_ndjfm_s = ds_ndjfm[['warm_days','warm_days_plus1','warm_days_plus2','degree_days',]].resample(time='AS-OCT').sum(min_count=5)
ds_ndjfm_recomb = xr.merge([ds_ndjfm_m,ds_ndjfm_s])
if os.path.exists(os.path.join(project_dir,'data','processed','era5','ndjfm_stats.nc')):
    os.remove(os.path.join(project_dir,'data','processed','era5','ndjfm_stats.nc'))
ds_ndjfm_recomb.to_netcdf(os.path.join(project_dir,'data','processed','era5','ndjfm_stats.nc'))
