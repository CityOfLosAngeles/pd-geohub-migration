
## Importing required packages

from sodapy import Socrata as sc
import pandas as pd
import json
import credentials as cd
import datetime as dt
import os
import geopandas as gpd
import arrest_functions as ar


def age_check(df):
    '''
    This function validates the age column in Arrests dfset. A flag will be raised the 
    age is less than 10 or greater than 90.
    '''
    df.age = df.age.astype(int)
    if min(df.age) > 10 & max(df.age) < 90:
        print('There are no outliers in age column')
        return 
    else:
        print("There are outliers in  column 'age', rows: {} {} ".format(df.age[df.age < 10].index.values, df.age[df.age > 90].index.values))
        return 
    
def validate_area_id(df):
    '''
    The LAPD has 21 Community Police Stations referred to as Geographic Areas within the department.
    These Geographic Areas are sequentially numbered from 1-21.
    '''
    df.area = pd.to_numeric(df.area)
    if min(df.area) <1 :
        print('There are outliers in area_id column. Rows are \n {}'.format(df[df.area == min(df.area)].index.values))
        return 
    elif max(df.area)>21:
        print('There are outliers in area_id column. Rows are \n {}'.format(df[df.area == max(df.area)].index.values))
        return
    else:
        print('No outliers in area_id column')
        return 
    
def validate_area_desc(df):
    '''
    Takes up the unique values from area column in df. Need proper validation scheme for this.
    '''
    val = [x for x in df.area_desc if x not in list_area]
    if len(val) ==0 :
        print('No Outliers found in area_description')
        return 
    else:
        print('Outliers found in area_description. Values: {}'.format(val))
        return 
    
    
def validate_arrst_typ_cd(df):
    '''
    A code to indicate the type of charge the individual was arrested for. D - Dependent,
    F - Felony I - Infraction M - Misdemeanor O - Other
    '''
    
    val = [x for x in df.arst_typ_cd if x not in list_arrst]
    if len(val) ==0 :
        print('No Outliers found in arrest_type_code column')
        return 
    else:
        print('Outliers found in arrest_type_code column: Values: {}'.format(val))
        return 
    

def validate_charge(df):
    '''
    Since no clear format for charge is given, I'll be using unique values of column for now
    '''
    val = [x for x in df.charge if x not in list_charge]
    if len(val) ==0 :
        print('No Outliers found charge column')
        return 
    else:
        print('Outliers found in charge column. Values: {}'.format(val))
        return 

def validate_charge_desc(df):
    '''
    Currently, I'm using the unique values in this column. A proper validation criteria is required.
    '''
    val = [x for x in df.chrg_desc if x not in list_charge_desc]
    if len(val) ==0 :
        print('No Outliers found in charge description column')
        return 
    else:
        print('Outliers found in charge description column. Values: {}'.format(val))
        return 
    

def validate_charg_cd(df):
    '''
    For now considering values between 00 and 99.
    
    NaN are converted to -1 and hence an outlier
    '''
    df.chrg_grp_cd = pd.to_numeric(df.chrg_grp_cd)
    
    df.chrg_grp_cd = df.chrg_grp_cd.fillna(value=-1)
    
    if min(df.chrg_grp_cd) < 1. :
        print('There are outliers in charge_code column. Rows are \n {}'.format(df[df.chrg_grp_cd == min(df.chrg_grp_cd)].index.values))
        return 
    elif max(df.chrg_grp_cd) > 99.:
        print('There are outliers in charge_code column. Rows are \n {}'.format(df[df.chrg_grp_cd == max(df.chrg_grp_cd)].index.values))
        return 
    else:
        print('No outliers in charge_code column')
        return 
    
    
def validate_crossstreet(df):
    '''
    The function validates if there are any NaN values in the Location_1 column
    '''
    
    if len(df.crsst[df.crsst.isna()]) == 0 :
        print('No outliers in cross_street column')
        return 
    else:
        
        df.crsst = df.crsst.fillna(value= -1)
        out = [x for x in range(len(df.crsst)) if df.crsst[x] == -1 ]
        print('Outliers found in cross_street column at row number: {}'.format(','.join([str(s) for s in out])))
        return 

    
def validate_descent(df):
    '''
    Descent Code: A - Other Asian B - Black C - Chinese D - Cambodian,
    F - Filipino G - Guamanian H - Hispanic/Latin/Mexican I - American Indian/Alaskan Native,
    J - Japanese K - Korean L - Laotian O - Other P - Pacific Islander S - Samoan,
    U - Hawaiian V - Vietnamese W - White X - Unknown Z - Asian Indian
    '''
    list_descent = ['A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'J', 'K' , 'L', 'O', 'P', 'S', 'U', 'V', 'W', 'X', 'Z']
    
    val = [x for x in df.descent_cd if x not in list_descent]
    if len(val) ==0 :
        print('No Outliers found in descent column')
        return 
    else:
        print('Outliers found in descent column. Values: {}'.format(val))
        return 
    
    
    
def validate_grp_desc(df):
    '''
    Currently, I'm using the unique values in this column. A proper validation criteria is required.
    '''
    
    val = [x for x in df.grp_description if x not in list_charge_grp_desc]
    if len(val) ==0 :
        print('No Outliers found in group_description column')
        return 
    else:
        print('Outliers found in group_description column. Values : {}'.format(','.join([str(s) for s in val])))
        return 
    

def validate_rep_dist(df):
    '''
    Socrata portal redirects user to a geojson file for list of reporting districts. I've collected unique values
    of REPDIST from that file and stored it in list list_repdist
    '''
    update_list = [str(i).zfill(4) for i in list_repdist]
    val = [x for x in df.rd if x not in update_list]
    if len(val) ==0 :
        print('No Outliers found in reporting_district column')
        return 
    else:
        print('Outliers found in reporting_district column. Values : {}'.format(','.join([str(s) for s in list(set(val))])))
        return 
    
    
def validate_report_id(df):
    '''
    Assuming the ids are unique, I'm applying that validation
    '''
    
    if len(df.rpt_id) == len(list(set(df.rpt_id))):
        print('No outliers in report_id column')
        return 
    else:
        count = df.rpt_id.value_counts()
        print('Non-unique records found in report_id column. Values: {}'.format(','.join([count[count>1].index.values[0]])))
        return 
    
    
def validate_sex(df):
    '''
    Validates the sex column for valid sex type from the list below.
    M- Male, F-Female
    '''
    list_sex = ['F', 'M']
    
    val = [i for i in range(len(df.sex_cd)) if df.sex_cd[i] not in list_sex]
    if len(val) ==0 :
        print('No Outliers found in sex column')
        return 
    else:
        print('Outliers found in sex column at row number: {}'.format(','.join([str(s) for s in val])))
        return 
    
    
def validate_time(df):
    '''
    This function validates the 'time' column. If the values are less than 0000 or greater than 2400
    or NaN, the outlier message will be displayed
    '''
    
    df.time= df.time.fillna(value= '-1' )
    truth_list = ['-1' if (str(i) < '0000' or str(i) > '2400') else i for i in df.time]
    
    if '-1' not in truth_list :
        print('No outliers in time column')
        return 
    else:
        #out = [x for x in range(len(df.time)) if (str(df.time[x]) < '0000' or str(df.time[x]) > '2400' )]
        out = [x for x in range(len(truth_list)) if truth_list[x] == '-1' ]
        print('Outliers found in time column at row number: {}'.format(','.join([str(s) for s in out])))
        return 
    
    
def validate_location(df):
    '''
    The function validates if there are any NaN values in the Location_1 column
    '''
    
    if len(df.location_1[df.location_1.isna()]) == 0 :
        print('No outliers in location column')
        return 
    else:
        
        df.location_1 = df.location_1.fillna(value= -1)
        out = [x for x in range(len(df.location_1)) if df.location_1[x] == -1 ]
        print('Outliers found in location column at row number: {}'.format(','.join([str(s) for s in out])))
        return 
    
    
    
    
def main():
    
    ## Establishing connectoin with Socrata to download the data
    ## To access the code, you'll have to create aan account with socrata developer portal
    ## My credentials are stored in credentials file that I've imported here
    client = sc(domain = "data.lacity.org", app_token = cd.app_token, username = cd.username, password = cd.password)
    
    results = client.get("7qi3-mqr5", limit=1000000)
    data = pd.DataFrame.from_records(results)
    
    ## Downloading files for reporting districts
    os.system("wget https://opendata.arcgis.com/datasets/4398360b1a0242b78904f46b3786ae73_0.geojson")
    
    gpd_data = gpd.GeoDataFrame.from_file('4398360b1a0242b78904f46b3786ae73_0.geojson')
    
    ##Defining gloval variables and assigning values to them
    global list_area, list_arrst, list_charge, list_charge_desc, list_charge_grp_desc, list_repdist
    list_area = data.area_desc.unique().tolist()
    list_arrst = ['D', 'F', 'I', 'M', 'O']
    list_charge = data.charge.unique().tolist()
    list_charge_desc = data.chrg_desc.unique().tolist()
    list_charge_grp_desc = data.grp_description.unique().tolist()
    list_repdist = gpd_data.REPDIST.unique().tolist()
    
    
    ## Calling functions
    
    age_check(data)
    validate_area_id(data)
    validate_area_desc(data)
    validate_arrst_typ_cd(data)
    validate_charge(data)
    validate_charge_desc(data)
    validate_charg_cd(data)
    # validate_crossstreet(data) ## This column has too many NaN values
    validate_descent(data)
    validate_grp_desc(data)
    validate_rep_dist(data)
    validate_report_id(data)
    validate_sex(data)
    validate_time(data)
    validate_location(data)
    

if __name__ == "__main__":
    main()