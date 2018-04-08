## Importing required packages
from sodapy import Socrata as sc
import pandas as pd
import json
import credentials as cd

client = sc(domain = "data.lacity.org", app_token = cd.app_token, username = cd.username, password = cd.password)

results = client.get("im35-exj5", limit=1000000000)

data = pd.DataFrame.from_records(results)

data.to_csv('calls_for_service_2017.txt',sep='|', index=False, header=True)


## For multiple files

## NO RUN

#dictionary = {'crime_2010_ytd':'7fvc-faax', 'call_for_servive_2018':'m9qm-gwm5'}

#for i in dictionary.keys():
#    results_i = client.get(dictionary[i], limit=1000000000)
#    data_i = pd.DataFrame.from_records(results_i)
#    print('Number of records in the file: {}'.format(len(data_i)))
#    data_i.to_csv('{}.txt'.format(i),sep='|', index=False, header=True)