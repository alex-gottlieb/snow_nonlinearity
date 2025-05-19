#!/usr/bin/env python
# coding: utf-8

import os
import xarray as xr
import numpy as np
import geopandas as gpd
import warnings
warnings.filterwarnings("ignore")

root_dir = '/dartfs-hpc/rc/lab/C/CMIG'
project_dir = os.path.join(root_dir,'agottlieb','snow_nonlinearity')
ds = xr.open_dataset(os.path.join(project_dir,'data','interim','snotel.nc'))
ds['tavg_warm'] = (ds['tavg']>0).where(ds['tavg'].notnull())
ds['tmax_warm'] = (ds['tmax']>0).where(ds['tmax'].notnull())
ds['tmin_warm'] = (ds['tmin']>0).where(ds['tmin'].notnull())
ds['sc'] = (ds['swe']>0).where(ds['swe'].notnull())



ds_ndjfm = ds.where((ds['time.month']>=11)|(ds['time.month']<=3))
ndjfm_tavg = ds_ndjfm['tavg'].resample(time='AS-OCT').mean()
ndjfm_tavg.name = 'tavg'
ndjfm_tvar = ds_ndjfm['tavg'].resample(time='AS-OCT').std()
ndjfm_tvar.name = 'tavg_std'
tvar_all = ds_ndjfm['tavg'].sel(time=slice("1998-10-01","2023-03-31")).std("time")
tvar_all.name = 'tavg_std_all'
ndjfm_warm = ds_ndjfm['tavg_warm'].resample(time='AS-OCT').sum(min_count=145)
ndjfm_warm.name = 'warm_days_tavg'
ndjfm_warm_tmax = ds_ndjfm['tmax_warm'].resample(time='AS-OCT').sum(min_count=145)
ndjfm_warm_tmax.name = 'warm_days_tmax'
ndjfm_warm_tmin = ds_ndjfm['tmin_warm'].resample(time='AS-OCT').sum(min_count=145)
ndjfm_warm_tmin.name = 'warm_days_tmin'
ndjfm_degrees = ds_ndjfm['tavg'].where(ds_ndjfm['tavg']>0).resample(time='AS-OCT').sum(min_count=85)
ndjfm_degrees.name = 'degree_days'
ndjfm_sc = ds_ndjfm['sc'].resample(time='AS-OCT').sum(min_count=145)
wy_sc = ds['sc'].resample(time='AS-OCT').sum(min_count=145)
wy_sc.name = 'wy_sc'
mar_swe = ds['swe'].where(ds['time.month']==3).resample(time='AS-OCT').mean()
mar_swe.name = 'mar_swe'
apr1_swe = ds['swe'].where((ds['time.month']==4)&(ds['time.day']==1)).resample(time='AS-OCT').mean()
apr1_swe.name = 'apr1_swe'
peak_swe = ds['swe'].resample(time='AS-OCT').max()
peak_swe.name = 'peak_swe'
sf = ds_ndjfm['ppt'].where(ds['tavg']<=0).resample(time='AS-OCT').sum()
sf.name = 'snow'
rf = ds_ndjfm['ppt'].where(ds['tavg']>0).resample(time='AS-OCT').sum()
rf.name = 'rain'

def calc_melt(ts):
    if np.isnan(ts).all():
        return np.nan,np.nan,np.nan,np.nan
    diff = ts[1:]-ts[:-1]
    peak = np.nanargmax(ts)
    n_melt = (diff[:peak]<0).sum().astype(float)
    total_melt_pre = diff[:peak][diff[:peak]<0].sum()
    total_accum_pre = diff[:peak][diff[:peak]>0].sum()
    total_melt = diff[diff<0].sum()
    return n_melt,total_melt_pre,total_accum_pre,total_melt

n_melt,total_melt_pre,total_accum_pre,total_melt=xr.apply_ufunc(calc_melt,ds['swe'].resample(time='AS-OCT'),input_core_dims=[['time']],output_core_dims=[[],[],[],[]],vectorize=True)
n_melt = n_melt.rename({'__resample_dim__':'time'})
n_melt.name = 'melt_days_prepeak'
total_melt_pre = total_melt_pre.rename({'__resample_dim__':'time'})
total_melt_pre.name = 'total_melt_prepeak'
total_accum_pre = total_accum_pre.rename({'__resample_dim__':'time'})
total_accum_pre.name = 'total_accum_prepeak'
total_melt = total_melt.rename({'__resample_dim__':'time'})
total_melt.name = 'total_melt'

ndjfm_recomb = xr.merge([ndjfm_tavg,ndjfm_tvar,tvar_all,
                         ndjfm_warm,ndjfm_warm_tmax,ndjfm_warm_tmin,ndjfm_degrees,
                         ndjfm_sc,wy_sc,
                         mar_swe,apr1_swe,peak_swe,
                         n_melt,total_melt_pre,total_accum_pre,total_melt,
                         rf,sf])

snotel_meta = gpd.read_file(os.path.join(project_dir,'data','processed','station_meta','snotel'))
lat_dict = dict(zip(snotel_meta['site'],snotel_meta['geometry'].y))
lon_dict = dict(zip(snotel_meta['site'],snotel_meta['geometry'].x))
ndjfm_recomb = ndjfm_recomb.assign_coords(lat=(("site"),[lat_dict[s] for s in ndjfm_recomb['site'].values]),
                                          lon=(("site"),[lon_dict[s] for s in ndjfm_recomb['site'].values]),)

if os.path.exists(os.path.join(project_dir,'data','processed','snotel','ndjfm_stats.nc')):
    os.remove(os.path.join(project_dir,'data','processed','snotel','ndjfm_stats.nc'))
ndjfm_recomb.to_netcdf(os.path.join(project_dir,'data','processed','snotel','ndjfm_stats.nc'))
