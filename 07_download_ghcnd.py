#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd

root_dir = '/dartfs-hpc/rc/lab/C/CMIG'
ghcn_dir = os.path.join(root_dir,'Data','Observations','GHCN')
with open(os.path.join(ghcn_dir,'ghcnd-inventory.txt'),'r') as src:
    lines = src.readlines()
ghcn_meta = [l.strip().split() for l in lines]
ghcn_meta = pd.DataFrame(ghcn_meta,columns=['staid','lat','lon','var','start','end'])
dtypes = ['object','float','float','object','int','int']
for i,c in enumerate(ghcn_meta.columns):
    ghcn_meta[c] = ghcn_meta[c].astype(dtypes[i])
ghcn_sd = ghcn_meta[ghcn_meta['var']=='SNWD']
ghcn_keep_sd = ghcn_sd[(ghcn_sd['end']>=2020)&(ghcn_sd['start']<=1997)&(ghcn_sd['lat']>0)]
ghcn_tmax = ghcn_meta[ghcn_meta['var']=='TMAX']
ghcn_keep_tmax = ghcn_tmax[(ghcn_tmax['end']>=2020)&(ghcn_tmax['start']<=1997)&(ghcn_tmax['lat']>0)]
ghcn_tmin = ghcn_meta[ghcn_meta['var']=='TMIN']
ghcn_keep_tmin = ghcn_tmin[(ghcn_tmin['end']>=2020)&(ghcn_tmin['start']<=1997)&(ghcn_tmin['lat']>0)]
keep_ids = list(set(ghcn_keep_sd['staid']) & set(ghcn_keep_tmax['staid']) & set(ghcn_keep_tmin['staid']))
base_url = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/STAID.csv.gz'
urls = [base_url.replace("STAID",s) for s in keep_ids]
with open(os.path.join(root_dir,'agottlieb','snow_nonlinearity','data','raw','ghcnd','urls.txt'),'w') as dst:
    for x in urls:
        dst.write(x+"\n")