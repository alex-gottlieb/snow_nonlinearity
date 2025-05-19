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
ims_dir = os.path.join(project_dir,'data','interim','ims_24km_nc')
ims_files = [os.path.join(ims_dir,f) for f in os.listdir(ims_dir)]
ims_files.sort()

ds = xr.concat([xr.open_dataset(f) for f in ims_files],dim='time')
sc = xr.where((ds['sc']!=2)&(ds['sc']!=4),np.nan,ds['sc'])
sc = xr.where(sc==4,1,sc)
sc = xr.where(sc==2,0,sc)

# sc_clim = sc.groupby("time.dayofyear")
# wy_sc = sc.resample(time='AS-OCT').count()
# wy_sc.to_netcdf(os.path.join(project_dir,'data','processed','ims_24km','wy_sc.nc'))

# djf_sc = sc.where(sc['time.season']=='DJF').resample(time='AS-OCT').count()
# djf_sc.to_netcdf(os.path.join(project_dir,'data','processed','ims_24km','djf_sc.nc'))

# ndjfm_sc = sc.where((sc['time.month']>=11)|(sc['time.month']<=3)).resample(time='AS-OCT').count()
# ndjfm_sc.to_netcdf(os.path.join(project_dir,'data','processed','ims_24km','ndjfm_sc.nc'))

mon_sc = sc.resample(time='1M').count()
mon_sc.to_netcdf(os.path.join(project_dir,'data','processed','ims_24km','mon_sc.nc'))
