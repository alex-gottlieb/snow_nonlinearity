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
data_dir = os.path.join(root_dir,'Data','Observations')
ims_dir = os.path.join(data_dir,'ims_snow_cover')
ims_files = [os.path.join(ims_dir,'24km',f) for f in os.listdir(os.path.join(ims_dir,'24km'))]
ims_files.sort()

out_dir = os.path.join(project_dir,'data','interim','ims_24km_nc')
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
  
y = sys.argv[1]

if os.path.exists(os.path.join(out_dir,f'{y}.nc')):
    exit()
    
coord_dir = os.path.join(ims_dir,'coords')
lat_f = os.path.join(coord_dir,'imslat_24km.bin')
lon_f = os.path.join(coord_dir,'imslon_24km.bin')

# lats and lons to map onto grid
nx = ny = 1024
with open(lat_f,'rb') as src:
    lats = np.fromfile(lat_f,dtype='<f',count=nx*ny)
    lats = np.reshape(lats,[nx,ny],order='F')    
with open(lon_f,'rb') as src:
    lons = np.fromfile(lon_f,dtype='<f',count=nx*ny)
    lons = np.reshape(lons,[nx,ny],order='F')    
    lons = (lons+ 270) % 360 - 180
# ERA5 data as reference for regridding
era5_dir = os.path.join(data_dir,'ERA5','daily')
era5_ref_f = os.path.join(era5_dir,'tasmean_2023.nc')
ref_ds = xr.open_dataset(era5_ref_f)
ref_ds = ref_ds.rename({"latitude":"lat","longitude":"lon"})
ref_ds = ref_ds.sel(lat=slice(90,0.25))
ref_ds['lon'] = (ref_ds['lon'] + 180) % 360 - 180
ref_ds = ref_ds.sortby("lon")
ref_ds = ref_ds.sortby("lat")

    
y_f = [f for f in ims_files if f[-27:-23]==str(y)] # deal with one year at a time
all_data = []
times = []
for f in y_f:
    with open(f,'r') as src:
        lines = src.readlines()
    try:
        data = []
        for l in lines[30:]:
            data.append([int(x) for x in l.strip()])
        all_data.append(data)
        times.append(pd.to_datetime(f[-27:-20],format='%Y%j'))
    except:
        continue
data = np.array(all_data)
data = xr.DataArray(data,coords=dict(time=times,x=np.arange(nx),y=np.arange(ny)),dims=['time','x','y'],name='sc')
data = data.assign_coords(dict(lat=(('x','y'),lats),lon=(('x','y'),lons)))
data['lon'].attrs['standard_name'] = 'longitude'
data['lat'].attrs['standard_name'] = 'latitude'
    
weight_fn = os.path.join(project_dir,'data','interim','ims24km_era5_weights.nc')
if not os.path.exists(weight_fn):
    regridder = xe.Regridder(data,ref_ds,'nearest_s2d')
    regridder.to_netcdf(weight_fn)
else:
    regridder = xe.Regridder(data,ref_ds,'nearest_s2d',weights=weight_fn)
data_regrid = regridder(data)
data_regrid = data_regrid.astype(np.uint8)
data_regrid.name = 'sc'
data_regrid['lon'] = (data_regrid['lon'] + 270) % 360 - 180
data_regrid = data_regrid.sortby("lon")
encoding={"sc":{"_FillValue":255,'zlib': True,'shuffle': True, 'complevel': 4,}}
data_regrid.to_netcdf(os.path.join(out_dir,f'{y}.nc'),encoding=encoding)
