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
#import numpy as np
import os
from zipfile import ZipFile
import geopandas as gpd
#import cStringIO

indir = r"F:\working\state_raw"
#final_crs = 4269 # EPSG value for bgLat/bgLon; NAD83
final_crs = 4326 # EPSG value for bgLat/bgLon; 4326 for WGS84: Google maps


def reproject(df,input_epsg=4267):
    # creates bgLat/lon that is standardized to final_crs
    print(' -- re-projecting location points')
    if input_epsg==final_crs:
        print('    -- no reprojection required')
        return df
    t = gpd.GeoDataFrame(df, 
                               geometry= gpd.points_from_xy(df.stLongitude,
                                                            df.stLatitude,
                                                            crs=input_epsg))
    t.to_crs(final_crs,inplace=True)
    t['stLatitude'] = t.geometry.y
    t['stLongitude'] = t.geometry.x
    return t[['api10','stLongitude','stLatitude']]

def clean_latlon(df):
    # dump records that are non numeric
    df.stLongitude = pd.to_numeric(df.stLongitude,errors='coerce')
    df.stLatitude = pd.to_numeric(df.stLatitude,errors='coerce')
    c = df.stLongitude.notna() & df.stLatitude.notna()
    return df[c]

def get_alabama():
    print(' - working on Alabama')
    zfn = os.path.join(indir,"alabama","well.zip!temp/out/WebWell_prj.shp")
    df = gpd.read_file(zfn)
    #print(df.API.head())
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_alaska():
    print(' - working on Alaska')
    zfn = os.path.join(indir,"alaska","AOGCC_Well_Surface_Location.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    #print(df.APINumber.head())
    #df['APINumber'] = df.api.astype('str')
    # print(df[['api','APINumber']].head())
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.APINumber.str[:10]
    # print(df[['api10','stLatitude']].head())
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_arkansas():
    print(' - working on Arkansas')
    zfn = os.path.join(indir,"arkansas","OIL_AND_GAS_WELLS_AOGC.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    #print(df.head(1).T)
    # There appears to be something wrong with the geometry of the shapefile...
    # df['stLongitude'] = df.geometry.x
    # df['stLatitude'] = df.geometry.y
    # ... so we use the explicit dbf coordinates instead
    df['stLongitude'] = df.longitude
    df['stLatitude'] = df.latitude
    df['api10'] = df.api_wellno.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]


def get_california():
    print(' - working on California')
    zfn = os.path.join(indir,"California","AllWells_gis.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    #print(df.API.head())
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API.str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_colorado():
    # have to pull the list from a zipped shape file
    print(' - working on Colorado')
    zf = os.path.join(indir,"colorado","WELLS_SHP.ZIP") 
    nzf = os.path.join(indir,"colorado","Wells_shp.zip")
    os.rename(zf,nzf)
    #"!OGWells_statewide.shp")
    df = gpd.read_file(nzf)
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API_Label.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_kansas():
    print(' - working on Kansas')
    zfn = os.path.join(indir,"kansas","OILGAS_WELLS_GEO27.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    #print(df.head(1).T)
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API_NUMBER.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]



# def get_louisiana_old():
#     print(' - working on Louisiana')
#     # fn = os.path.join(indir,"louisiana","LA_well_info.csv")
#     fn = os.path.join(indir,"louisiana","results.csv")
#     df = pd.read_csv(fn,low_memory=False,
#                      usecols = ['API Num','Longitude','Latitude'],
#                      dtype={'API Num':str},encoding='cp1252')
#     df = df[['API Num','Longitude','Latitude']]
#     df.columns = ['APINumber','stLongitude','stLatitude']
#     df = clean_latlon(df)
#     df = df[df.APINumber.str.len()>8]  # dump rows with no APINumber
#     df = df[df.APINumber.str[:2]=='17'] # dump all zeros and Texas!
#     df['api10'] = df.APINumber.str.replace('-','').str[:10]
#     df = df.sort_values('api10')
#     df = df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]
#     return reproject(df)

def get_louisiana():
    print(' - working on Louisiana')
    zfn = os.path.join(indir,"louisiana","8866.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    # print(df.columns)

    df = df[df.API_NUM.str.len()>8]  # dump rows with no APINumber
    df = df[df.API_NUM.str[:2]=='17'] # dump all zeros and Texas!
    df['api10'] = df.API_NUM.str.replace('-','').str[:10]

    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_mississippi():
    print(' - working on Mississippi')
    zfn = os.path.join(indir,"mississippi","Wells.csv")
    df = pd.read_csv(zfn,dtype={'API10':str})
    #print(df.columns)
    #df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df['Long(NAD83)']
    df['stLatitude'] = df['Lat(NAD83)']
    df['api10'] = df.API10.str[:10]
    df = clean_latlon(df)
    #print(df.head())
    out = df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]
    return reproject(out,4269)

def get_montana():
    print(' - working on Montana')
    fn = os.path.join(indir,"montana\location.csv")
    df = pd.read_csv(fn,sep='\t',low_memory=False)
    df = df[['API #','Wh_Long','Wh_Lat']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df = clean_latlon(df)
    df['api10'] = df.APINumber.str.replace('-','').str[:10]
    #print(df[df.api10.duplicated(keep=False)])
    #print(df.head())
    df = df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]
    return reproject(df)


def get_new_mexico():
    print(' - working on New Mexico')
    zfn = os.path.join(indir,"new mexico","New_Mexico_Oil_and_Gas_wells.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.id.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_north_dakota():
    print(' - working on North Dakota')
    zfn = os.path.join(indir,"north dakota","OGD_Wells.zip")
    df = gpd.read_file(zfn)
    #print(df[['api_no']].head())
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.api_no.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_ohio():
    # have to pull the list from a zipped shape file
    print(' - working on Ohio')
    zfn = os.path.join(indir,"ohio","OGWells_statewide.zip!OGWells_statewide.shp")
    #df = gpd.read_file(zf,ignore_geometry=True)
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API_NO.str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]


def get_oklahoma():
    print(' - working on Oklahoma')
    zfn = os.path.join(indir,"oklahoma","RBDMS_WELLS.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    # print(df.columns)
    df['APINumber'] = df.api.astype('str')
    #print(df[['api','APINumber']].head())
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.APINumber.str[:10]
    #print(df[['api10','stLatitude']].head())
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_pennsylvania():
    print(' - working on Pennsylvania')
    fn = os.path.join(indir,"pennsylvania","OilGasWellInventory.csv")
    df = pd.read_csv(fn,low_memory=False)
    df = df[['API','LONGITUDE_DECIMAL','LATITUDE_DECIMAL']]
    df.columns = ['APINumber','stLongitude','stLatitude']
    df = clean_latlon(df)
    df = df[df.APINumber.str.len()==9]  # dump rows with odd APINumber
    df['api10'] = '37'+df.APINumber.str.replace('-','')# .str[:8]
    # print(df[df.api10.duplicated(keep=False)])
    # print(df.head())
    # print(len(df))
    df = df[df.stLatitude.notna()]
    # print(len(df))
    df = df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]
    return reproject(df,4269) #NAD83



def get_texas():
    # have to pull the list from a zipped collection of shape files
    # IN the zip the 'b' files are the bottom well characteristics, 's'
    # are the surface, which is probably what we want
    print(' - working on Texas')
    zf = os.path.join(indir,"texas","texas_wells_documents_20220918.zip")
    #tmp_dbf = './tmp/temp.dbf'
    tmp_zip = './tmp/temp.zip'
    
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
            # dbf_fn = f'well{ID}b.dbf'
            with zbig.open(zname)as z2:
                with open(tmp_zip,'wb') as tmp: # write the nested zip to temp file
                    tmp.write(z2.read())
            # shp_name = tmp_zip+f'!well{ID}b.shp'
            shp_name = tmp_zip+f'!well{ID}s.shp'
            df = gpd.read_file(shp_name)
            df = df.to_crs(final_crs) #reproject
            df['apilen'] = df.API.str.len()
            df = df[df.apilen>5]
            df['api10'] = '42'+ df.API.str[:8]
            df['stLongitude'] = df.geometry.x
            df['stLatitude'] = df.geometry.y
            df = clean_latlon(df)
            df = df[['api10','stLongitude','stLatitude']]
            alldf.append(df)
                                 
    out = pd.concat(alldf)    
    out = out[out.api10.str.strip().str.len()==10]
    return out[~out.api10.duplicated()]
    #return reproject(out,4326)

def get_texas_FracTracker():
    # alternative data source? or same?
    df = pd.read_csv(r"F:\working\state_raw\texas\FracTrackerNationalWells_Part3_TX.csv",
                     dtype={'API':str})
    df['api10'] = '42' + df.API.str[:8]
    df['stLongitude'] = df.Long 
    df['stLatitude'] = df.Lat 
    df = clean_latlon(df)
    df = df[['api10','stLongitude','stLatitude']]
    return df[~df.api10.duplicated()]

def get_utah():
    print(' - working on Utah')
    zfn = os.path.join(indir,"utah","Utah_Oil_and_Gas_Well_Locations.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    #print(df.API.head())
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.API.str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_virginia():
    print(' - working on Virginia')
    zfn = os.path.join(indir,"virginia","frmPrint.xls")
    df = pd.read_excel(zfn)
    df.columns = ['File','OperatorName','Operation','APINumber','Zone',
                  'Y','X','Quad','Type','County','Depth']
    df['api10'] = df.APINumber.astype('str').str[:10]
    # projection is probably 6595, maybe 2284
    gdf = gpd.GeoDataFrame(df, 
                           geometry= gpd.points_from_xy(df.X,df.Y,crs=6595))
    gdf = gdf.to_crs(final_crs)
    gdf['stLongitude'] = gdf.geometry.x
    gdf['stLatitude'] = gdf.geometry.y
    gdf = clean_latlon(gdf)
    #print(gdf.head(1).T)
    return gdf[~gdf.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_west_virginia():
    print(' - working on West Virginia')
    zfn = os.path.join(indir,"west virginia","oil_gas.zip")
    df = gpd.read_file(zfn)
    df = df.to_crs(final_crs) #reproject
    df['stLongitude'] = df.geometry.x
    df['stLatitude'] = df.geometry.y
    df['api10'] = df.api.str.replace('-','').str[:10]
    df = clean_latlon(df)
    return df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]

def get_wyoming():
    # Newest zip file should be named Wells.zip
    print(' - working on Wyoming')
    zname = os.path.join(indir,"wyoming","Wells.zip")
    # need to get the correct name of zipped file
    with ZipFile(zname) as zbig:
        int_fn = None
        for item in zbig.namelist():
            l = item.split('.')
            if l[0][-2:]=='WH':
                int_fn = item
                break
        if int_fn==None:
            print('Could not resolve internal file name for wyoming Wells.zip!')
            return pd.DataFrame()
    zf = zname + F'!{int_fn}'
    df = gpd.read_file(zf)
    df['api10'] = df.CAPINO.str.replace('-','').str[:10]
    df['stLongitude'] = df.LON
    df['stLatitude'] = df.LAT
    df = clean_latlon(df)
    df = df[~df.api10.duplicated()][['api10','stLongitude','stLatitude']]
    return reproject(df, 4269) # just a guess at this point)

    
def get_all():
    biglst = []
    biglst.append(get_alabama())
    biglst.append(get_alaska())
    biglst.append(get_arkansas())
    biglst.append(get_california())
    biglst.append(get_colorado())
    biglst.append(get_kansas())
    biglst.append(get_louisiana())
    biglst.append(get_mississippi())
    biglst.append(get_montana())
    biglst.append(get_new_mexico())
    biglst.append(get_north_dakota())
    biglst.append(get_ohio())
    biglst.append(get_oklahoma())
    biglst.append(get_pennsylvania())    
    biglst.append(get_texas())
    biglst.append(get_utah())
    biglst.append(get_virginia())
    biglst.append(get_west_virginia())
    biglst.append(get_wyoming())

    whole = pd.concat(biglst,sort=True)
    whole.to_csv(r"C:\MyDocs\OpenFF\data\external_refs\state_latlon.csv",
                  quotechar='$',encoding='utf-8')
    print('file saved to external_refs')
    return whole
    
if __name__ == '__main__':
    t = get_all()