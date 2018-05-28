## Importing required packages
import pandas as pd
import json
import credentials as cd
import numpy as np
import sys
import os
import geopandas as gpd
import shapely
import datetime as dt
import areas
try:
    from sodapy import Socrata as sc
except:
    os.system("pip install sodapy")
try:
    from sodapy import tika
except:
    os.system("pip install tika")
from tika import parser



def validate_date_reported(df):
    if len(df[df.date_occ > df.date_rptd])>0:
        print('Date reposrted cannot be less than the incident date. Anomalies in rows :{}'.format\
             (df[df.date_occ <= df.date_rptd].index.tolist()))
    else:
        print('No outliers in Date reported column')
    
    return



def validate_area_id(df):
    '''
    The LAPD has 21 Community Police Stations referred to as Geographic Areas within the department.
    These Geographic Areas are sequentially numbered from 1-21.
    '''
    df.area_id = pd.to_numeric(df.area_id)
    if min(df.area_id) <1 :
        print ('There are outliers in the data. Rows are \n', df[df.area_id == min(df.area_id)].index.values)
        
        return
    elif max(df.area_id)>21:
        print('There are outliers in the data. Rows are \n', df[df.area_id == max(df.area_id)].index.values)
        return
    else:
        print('No outliers in area column')
        return
    
def validate_area_name(df):
    '''
    Takes up the unique values from area column in data. Need proper validation scheme for this.
    '''
    val = [x for x in df.area_name if x not in list_area]
    if len(val) ==0 :
        print('No Outliers found in Area Name column.')
    else:
        print('Outliers found in Area Name column in rows: {}'.format(val))
        
    return


def validate_crime_cd_desc(df):
    '''
    Takes up the unique values from area column in data. Need proper validation scheme for this.
    '''
    val = [x for x in df.crm_cd_desc if x not in list_cr_cd_desc]
    if len(val) ==0 :
        print('No Outliers found in Crime Code Desc column.')
    else:
        print('Outliers found in Crime Code Desc column in rows: {}'.format(val))
        
    return


def validate_crossstreet(df):
    '''
    The function validates if there are any NaN values in the Cross Street column
    '''
    
    if len(df.cross_street[df.cross_street.isna()]) == 0 :
        print('No outliers in data')
        return
    else:
        
        df.cross_street = df.cross_street.fillna(value= -1)
        out = [x for x in range(len(df.cross_street)) if df.cross_street[x] == -1 ]
        print('Outliers found in at row number: {}'.format(','.join([str(s) for s in out])))
        return

def validate_dr_no(df):
    '''
    Assuming the ids are unique, I'm applying that validation
    '''
    
    if len(df.dr_no) == len(df.dr_no.unique().tolist()):
        print('No outliers in dr_no column')
    else:
        count = df.dr_no.value_counts()
        try:
            print('Non-unique records found in DR No. column. Rows {}'','.join([count[count>1].index.values[0]]))
        except IndexError:
            print('No outliers in dr_no column')
            
    return


def merge_list(list1, list2):
    resulting_list = list(list1)
    resulting_list.extend(x for x in list2 if x not in resulting_list)
    resulting_list = list(set(resulting_list))
    return resulting_list


def validate_location_1(df):
    '''
    
    This function extracts the lat and the lon provided in the dataset and checks if the points are within the 
    limits of the city of LA. 
    Note: This also includes the unknown locations that are for now geocoded as (0,0)
    
    '''
    df['lon'] = [df.location_1[i]['coordinates'][0] for i in range(len(df))]
    df['lat'] = [df.location_1[i]['coordinates'][1] for i in range(len(df))]
    
    ## Bounding Box for LA city (-118.66819,33.703621,-118.155296,34.337307)
    
    lon_list_1 = df[df.lon> -118.155296].index.tolist()
    lon_list_2 = df[df.lon< -118.66819].index.tolist()

    lat_list_1 = df[df.lat> 34.337307].index.tolist()
    lat_list_2 = df[df.lat< 33.703621].index.tolist()
    
    lon_list = merge_list(lon_list_1, lon_list_2)
    lat_list = merge_list(lat_list_1, lat_list_2)

    latlon_list = merge_list(lon_list, lat_list)
    
    print('Rows in the dataset not within the LA City boundary are : {}'.format(latlon_list))
    
    return

def validate_mo_code(df):
    '''
    This function uses the pre-defined MO codes and checks if the MO codes in data are part of this list.
    
    Note: Source of MO codes: https://data.lacity.org/api/views/y8tr-7khq/files/3a967fbd-f210-4857-bc52-60230efe256c?    download=true&filename=MO%20CODES%20(numerical%20or
    '''
    val = []
    for i in df.mocodes:
        try:
            for j in i.split(' '):
                if j not in moc:
                    val.append(j)
                else:
                    pass
        except AttributeError:
            pass
        
    if len(val) == 0 :
        print('No Outliers found in Crime Code Desc column.')
    else:
        print('Outliers found in Crime Code Desc column in rows: {}'.format(val))
        
    return

def validate_premise_cd(df):
    '''
    Takes up the unique values from premise code column in data. Need proper validation scheme for this.
    '''
    val = [x for x in df.premis_cd if x not in premis_cd_list]
    if len(val) ==0 :
        print('No Outliers found in Premise Code column.')
    else:
        print('Outliers found in Premise Code column in rows: {}'.format(val))
        
    return


def validate_premise_cd_desc(df):
    '''
    Takes up the unique values from premise code desc column in data. Need proper validation scheme for this.
    '''
    val = [x for x in df.premis_desc if x not in premis_desc_list]
    if len(val) ==0 :
        print('No Outliers found in Premise Code Desc column.')
    else:
        print('Outliers found in Premise Code Desc column in rows: {}'.format(val))
        
    return


def validate_rep_dist(df):
    '''
    Socrata portal redirects user to a geojson file for list of reporting districts. I've collected unique values
    of REPDIST from that file and stored it in list list_repdist
    '''
    update_list = [str(i).zfill(4) for i in list_repdist]
    val = [x for x in df.rpt_dist_no if x not in update_list]
    if len(val) ==0 :
        print('No Outliers found in Reposting Dstrict column.')
    else:
        print('Outliers found in Reposting DIstrict column. Values : {}'.format(','.join([str(s) for s in list(set(val))])))
    
    return

def validate_status_cd(df):
    '''
    Takes up the unique values from status code column in data.
    '''
    val = [x for x in df.status if x not in status_list]
    if len(val) ==0 :
        print('No Outliers found in Status Code column.')
    else:
        print('Outliers found in Status Code column in rows: {}'.format(val))
        
    return


def validate_status_cd_desc(df):
    '''
    Takes up the unique values from status code desc column in data.
    '''
    val = [x for x in df.status_desc if x not in status_desc_list]
    if len(val) ==0 :
        print('No Outliers found in Status Code desc column.')
    else:
        print('Outliers found in Status Code desc column in rows: {}'.format(val))
        
    return


def validate_time(df):
    '''
    This function validates the 'time' column. If the values are less than 0000 or greater than 2400
    or NaN, the outlier message will be displayed
    '''
    
    df.time_occ= df.time_occ.fillna(value= '-1' )
    truth_list = ['-1' if (str(i) < '0000' or str(i) > '2400') else i for i in df.time_occ]
    
    if '-1' not in truth_list :
        print('No outliers in data')
    else:
        #out = [x for x in range(len(df.time)) if (str(df.time[x]) < '0000' or str(df.time[x]) > '2400' )]
        out = [x for x in range(len(truth_list)) if truth_list[x] == '-1' ]
        print('Outliers found in at row number: {}'.format(','.join([str(s) for s in out])))
        
    return


def validate_age(df):
    '''
    This function validates the age column in Arrests dataset. A flag will be raised the 
    age is less than 10 or greater than 90.
    
    Note: All NaN values are converted to -1
    
    '''
    df.vict_age= df.vict_age.fillna(value= '-1' )
    df.vict_age = df.vict_age.astype(int)
    if min(df.vict_age) > 10 & max(df.vict_age) < 90:
        print('No Outliers found in Victim Age column')
    else:
        print("Outliers found in Victim Age column. Rows: ", df.vict_age[df.vict_age < 10].index.values.tolist(),\
                df.vict_age[df.vict_age > 90].index.values.tolist())
        
    return


def validate_descent(df):
    '''
    Descent Code: A - Other Asian B - Black C - Chinese D - Cambodian,
    F - Filipino G - Guamanian H - Hispanic/Latin/Mexican I - American Indian/Alaskan Native,
    J - Japanese K - Korean L - Laotian O - Other P - Pacific Islander S - Samoan,
    U - Hawaiian V - Vietnamese W - White X - Unknown Z - Asian Indian
    '''
    list_descent = ['A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'J', 'K' , 'L', 'O', 'P', 'S', 'U', 'V', 'W', 'X', 'Z']
    
    val = [x for x in df.vict_descent if x not in list_descent]
    if len(val) ==0 :
        print('No Outliers found in Victim Descent column.')
    else:
        print('Outliers found in Victim Descent column. Values: {}'.format(val))
        
    return


def validate_sex(df):
    '''
    Validates the sex column for valid sex type from the list below.
    M- Male, F-Female
    '''
    list_sex = ['F', 'M']
    
    val = [i for i in range(len(df.vict_sex)) if df.vict_sex[i] not in list_sex]
    if len(val) ==0 :
        print( 'No Outliers found in Victim Sex column.')
    else:
        print( 'Outliers found in Victim Sex column at row number: {}'.format(','.join([str(s) for s in val])))
        
    return


def validate_weapon_used_cd(df):
    '''
    Validates the weapon_used_cd column with the unique values currently in the column.
    '''
    
    val = [x for x in df.weapon_used_cd if x not in weapon_cd_list]
    if len(val) ==0 :
        print('No Outliers found in Weapon Used Code column.')
    else:
        print('Outliers found in Weapon Used Code column. Values: {}'.format(val))
        
    return

def validate_weapon_used_desc(df):
    '''
    Validates the weapon_used_cd column with the unique values currently in the column.
    '''
    
    val = [x for x in df.weapon_desc if x not in weapon_desc]
    if len(val) ==0 :
        print('No Outliers found in Weapon Used Desc column.')
    else:
        print('Outliers found in Weapon Used Desc  column. Values: {}'.format(val))
        
    return
    
    
    
def main():
    client = sc(domain = "data.lacity.org", app_token = cd.app_token, username = cd.username, password = cd.password)
    results = client.get("7fvc-faax", limit=10000000000)
    data = pd.DataFrame.from_records(results)
    
    ## Defining global variables
    global list_area
    global list_cr_cd_desc
    global moc
    global premis_cd_list
    global premis_desc_list
    global status_list
    global status_desc_list 
    global weapon_cd_list
    global weapon_desc
    global list_repdist
    
    ##Global lists
    status_list = ['AO', 'AA', 'JA', 'IC', 'JO']
    weapon_desc = data.weapon_desc.unique().tolist()
    weapon_desc.remove(np.nan)
    weapon_cd_list = ['400', '511', '500', '200', '308', '102', '212', '307', '111',
       '106', '207', '109', '306', '104', '122', '503', '304', '219',
       '101', '113', '218', '206', '108', '205', '504', '513', '512',
       '507', '216', '515', '115', '309', '221', '204', '312', '103',
       '302', '208', '217', '311', '107']
    status_desc_list = ['Adult Other', 'Adult Arrest', 'Juv Arrest', 'Invest Cont', 'Juv Other']
    premis_desc_list = data.premis_desc.unique().tolist()
    premis_cd_list = data.premis_cd.unique().tolist()
    list_cr_cd_desc = data.crm_cd_desc.unique().tolist()
    list_area = data.area_name.unique().tolist()
    
    
    ## Extracting MO Codes from the pdf file
    parsed = parser.from_file('MO_CODES__numerical_order_.pdf')
    mo_codes = pd.DataFrame(parsed['content'][65:].split('\n'))
    mo_codes = [parsed['content'][65:].split('\n')[i].split('  ')[0] for i in range(len(parsed['content'][65:].split('\n')))]
    moc = []
    for i in mo_codes:
        if type(pd.to_numeric(i, errors='coerce')) == np.int64:
            moc.append(i)
        else:
            pass
    
    ## Extracting data from reporting district shapefile
    gpd_data = gpd.GeoDataFrame.from_file('4398360b1a0242b78904f46b3786ae73_0.geojson')
    list_repdist = gpd_data.REPDIST.unique().tolist()
    
    
    ## Calling the functions now
    
    validate_date_reported(data)
    validate_area_id(data)
    validate_area_name(data)
    validate_crime_cd_desc(data)
    validate_crossstreet(data)
    validate_dr_no(data)
    validate_location_1(data)
    validate_mo_code(data)
    validate_premise_cd(data)
    validate_premise_cd_desc(data)
    validate_rep_dist(data)
    validate_status_cd(data)
    validate_status_cd_desc(data)
    validate_time(data)
    validate_age(data)
    validate_descent(data)
    validate_sex(data)
    validate_weapon_used_cd(data)
    validate_weapon_used_desc(data)
    

if __name__ == "__main__":
    main()