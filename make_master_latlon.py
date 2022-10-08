# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 20:52:54 2022

@author: Gary

This script uses data previously downloaded from several state databases
to create a single file of all wells from those states (by api10) and 
their given lat and lon.

This is meant to be used in Open-FF to compare and correct FracFocus lat/lon
data
"""

import pandas as pd
import numpy as np
import os
from zipfile import ZipFile
import geopandas as gpd

indir = r"F:\working\state_raw"

def get_louisiana():
    print(' - working on Louisiana')
    fn = os.path.join(indir,"louisiana","LA_well_info.csv")
    df = pd.read_csv(fn,low_memory=False,
                     dtype={'API':str},encoding='cp1252')
    df = df[['API Num','Longitude','Latitude']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df = df[df.APINumber.str.len()>8]  # dump rows with no APINumber
    df = df[df.APINumber.str[:2]=='17'] # dump all zeros and Texas!
    df['api10'] = df.APINumber.str.replace('-','').str[:10]
    df = df.sort_values('api10')
    # print(df[df.api10.duplicated(keep=False)][['api10','stLatitude','stLongitude']].head(40))
    
    # print(df.head())
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_montana():
    print(' - working on Montana')
    fn = os.path.join(indir,"montana\location.csv")
    df = pd.read_csv(fn,sep='\t',low_memory=False)
    df = df[['API #','Wh_Long','Wh_Lat']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df['api10'] = df.APINumber.str.replace('-','').str[:10]
    #print(df[df.api10.duplicated(keep=False)])
    #print(df.head())
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_new_mexico():
    print(' - working on New Mexico')
    fn = os.path.join(indir,"new mexico","New_Mexico_Oil_and_Gas_wells.csv")
    df = pd.read_csv(fn,low_memory=False)
    df = df[['id','X','Y']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df['api10'] = df.APINumber.str.replace('-','').str[:10]
    #print(df[df.api10.duplicated(keep=False)])
    #print(df.head())
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_pennsylvania():
    print(' - working on Pennsylvania')
    fn = os.path.join(indir,"pennsylvania","OilGasWellInventory.csv")
    df = pd.read_csv(fn,low_memory=False)
    df = df[['API','LONGITUDE_DECIMAL','LATITUDE_DECIMAL']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df = df[df.APINumber.str.len()==9]  # dump rows with odd APINumber
    df['api10'] = '37'+df.APINumber.str.replace('-','').str[:8]
    # print(df[df.api10.duplicated(keep=False)])
    # print(df.head())
    # print(len(df))
    df = df[df.stLatitude.notna()]
    # print(len(df))
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_texas():
    # have to pull the list from a zipped collection of shape files
    print(' - working on Texas')
    zf = os.path.join(indir,"texas","texas_wells_documents_20220918.zip")
    tmp_dbf = './tmp/temp.dbf'
    
    with ZipFile(zf) as zbig:
        IDs = []
        alldf = []
        #print(zbig.namelist())
        for item in zbig.namelist():
            IDs.append(item[4:7])
        #print(IDs)
        for i,ID in enumerate(IDs):
            if i%100==0:
                print(f'   -- Texas zipped files: {i}')
            zname = f'well{ID}.zip'
            dbf_fn = f'well{ID}b.dbf'
            with ZipFile(zbig.open(zname)) as zsmall:
                try:
                    with zsmall.open(dbf_fn) as f:
                        with open(tmp_dbf,'wb') as tmp:
                            tmp.write(f.read())
                        df = gpd.read_file(tmp_dbf).drop("geometry",axis=1)
                        df['apilen'] = df.APINUM.str.len()
                        df = df[df.apilen>5]
                        df['api10'] = df.APINUM.str[:10]
                        df = df[['api10','LONG27','LAT27']]
                        df.columns = ['api10','stLongitude','stLatitude']
                        alldf.append(df)
                except:
                    print(f'couldnt open ID= {ID}')
                                     
    out = pd.concat(alldf)    
    out = out[out.api10.str.strip().str.len()==10]
    out = out[~out.api10.duplicated()]
    return out

def get_west_virginia():
    print(' - working on West Virginia')
    fn = os.path.join(indir,"west virginia","oil_gas.csv")
    df = pd.read_csv(fn,low_memory=False,
                     dtype={'api':str})
    df = df[['api','X','Y']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df['api10'] = df.APINumber.str.replace('-','').str[:10]
    # print(df[df.api10.duplicated(keep=False)].sort_values('api10'))
    # print(df.head())
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

    
def get_all():
    biglst = []
    biglst.append(get_louisiana())
    biglst.append(get_montana())
    biglst.append(get_new_mexico())
    biglst.append(get_pennsylvania())    
    biglst.append(get_texas())
    biglst.append(get_west_virginia())

    whole = pd.concat(biglst,sort=True)
    whole.to_csv(r"C:\MyDocs\OpenFF\data\external_refs\state_latlon.csv",
                 quotechar='$',encoding='utf-8')
    print('file saved to external_refs')
    return whole
    
if __name__ == '__main__':
    t = get_all()