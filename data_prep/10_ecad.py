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

ecad_dir = os.path.join(root_dir,'Data','Observations','ECAD')
tmean_dir = os.path.join(ecad_dir,'tg')
tmean_files = os.listdir(tmean_dir)
tmean_sta = [s.split("_")[1].split(".")[0] for s in tmean_files if s.startswith("TG")]

tmin_dir = os.path.join(ecad_dir,'tn')
tmin_files = os.listdir(tmin_dir)
tmin_sta = [s.split("_")[1].split(".")[0] for s in tmin_files if s.startswith("TN")]

tmax_dir = os.path.join(ecad_dir,'tx')
tmax_files = os.listdir(tmax_dir)
tmax_sta = [s.split("_")[1].split(".")[0] for s in tmax_files if s.startswith("TX")]

sd_dir = os.path.join(ecad_dir,'sd')
sd_files = os.listdir(sd_dir)
sd_sta = [s.split("_")[1].split(".")[0] for s in sd_files if s.startswith("SD")]

ppt_dir = os.path.join(ecad_dir,'ppt')
ppt_files = os.listdir(ppt_dir)
ppt_sta = [s.split("_")[1].split(".")[0] for s in ppt_files if s.startswith("RR")]

shared_sta = list(set(tmean_sta)&set(sd_sta))
shared_sta.sort()

def to_xarray(s):
    tmean_f = os.path.join(tmean_dir,f'TG_{s}.txt')
    tmin_f = os.path.join(tmin_dir,f'TN_{s}.txt')
    tmax_f = os.path.join(tmax_dir,f'TX_{s}.txt')
    sd_f = os.path.join(sd_dir,f'SD_{s}.txt')
    ppt_f = os.path.join(ppt_dir,f'RR_{s}.txt')
    
    skip=[19,19,19,20,19]
    data = []
    for i,f in enumerate([tmean_f,tmin_f,tmax_f,sd_f,ppt_f]):
        try:
            df = pd.read_csv(f,skiprows=skip[i])
            df.columns = [c.strip() for c in df.columns]
            data.append(df)
        except:
            continue
    if len(data)<2:
        return None

    keep_vars = ['DATE','TG','TN','TX','SD','RR']
    merged = data[0][[c for c in data[0].columns if x in keep_vars]]
    for d in data[1:]:
        merged = merged.merge(d[[c for c in data[0].columns if x in keep_vars]],on='DATE')
    merged = merged.replace({-9999:np.nan})
    merged['DATE']=pd.to_datetime(merged['DATE'],format='%Y%m%d')
    if 'TG' in merged.columns:
        merged['TG'] = merged['TG']/10
    if 'TN' in merged.columns:
        merged['TN'] = merged['TN']/10
    if 'TX' in merged.columns:
        merged['TX'] = merged['TX']/10
    merged = merged.rename(columns={"DATE":"time","TG":"tavg","TN":"tmin","TX":"tmax","SD":"sd","RR":"ppt"})
    da = xr.Dataset.from_dataframe(merged.set_index("time"))
    da = da.assign_coords(site=s)
    return da



pool = Pool(8)
res = pool.map(to_xarray,shared_sta)
res = [r for r in res if r is not None]
res = xr.concat(res,dim='site')
if os.path.exists(os.path.join(project_dir,'data','interim','ecad.nc')):
    os.remove(os.path.join(project_dir,'data','interim','ecad.nc'))
res.to_netcdf(os.path.join(project_dir,'data','interim','ecad.nc'))
pool.close()
