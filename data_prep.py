import pandas as pd
import numpy as np
import glob
import os

import prepare_csv

from datetime import datetime
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')

file_path='/Users/katalinnikolics/futurae/data'

def get_folders_to_process():
    folders = list(glob.glob(file_path + '/*', recursive=True))
    folders = list(filter(lambda k: '.tar.gz' not in k, folders))
    folders = list(filter(lambda k: '.age' not in k, folders))
    folders = list(filter(lambda k: '.sh' not in k, folders))
    return folders

try:
    user_logins = pd.read_parquet('/Users/katalinnikolics/futurae/data_biz/datafiles/user_activity.parquet.gzip')
except:
    prepare_csv.process_all_files()
    user_logins = pd.read_parquet('/Users/katalinnikolics/futurae/data_biz/datafiles/user_activity.parquet.gzip')

user_logins['date'] = pd.to_datetime(user_logins['timestamp'], utc=True, unit='s')
user_logins['weekday'] = user_logins['date'].dt.day_name()
# add frequency
user_logins['week'] = user_logins['date'].dt.to_period("w")
user_logins['day'] = user_logins['date'].dt.to_period("D")
user_logins['month'] = user_logins['date'].dt.to_period("M")

user_logins = user_logins[user_logins.month > '2021-12']

user_logins['monthofyear'] = user_logins['date'].dt.month
user_logins['dayofweek'] = user_logins['date'].dt.dayofweek
user_logins["dayofyear"] = user_logins['date'].dt.dayofyear
user_logins["year"] = user_logins['date'].dt.year

print('\n *** User activity table read, date columns added.***\n\nJoining services/organisations...\n')

services = pd.read_csv('export-futurae-production-20220921-162153/services.csv')
service_cols = pd.read_csv('export-futurae-production-20220921-162153/services_headers.csv')
services.columns = service_cols.columns

orgs = pd.read_csv('export-futurae-production-20220921-162153/organisations.csv', header=None)
org_cols = pd.read_csv('export-futurae-production-20220921-162153/organisations_headers.csv', nrows=3)
orgs.columns = org_cols.columns

org_dict = dict(zip(orgs.organisation_id,orgs.name))
services['org_name'] = services['organisation_id'].map(org_dict)

service_dict = dict(zip(services.service_id,services.org_name))
user_logins['organisation'] = user_logins['service_id'].map(service_dict)

print('Now filter out for time period, trial environment and test organisations...')
tmp_services = services[services.trial==False]
logins = pd.merge(user_logins, tmp_services[['service_id', 'name','active']], on='service_id', how='right')

del user_logins #free up some memory
logins = logins[~logins.organisation.isna()]
#logins = logins[logins.month > '2020-12']
logins = logins[logins.organisation != 'Futurae Admin']
logins = logins.dropna(subset=['organisation'])
logins = logins[~logins.organisation.str.contains('test')]
logins['authentications'] = logins['success'].replace([True,False],[1,0])
print('...done.')
print('Dataframe shape:',logins.shape[0],'\n')

print('Fixing strings in type column...\n')
logins['factor'] = logins['factor'].str.replace('\\', '')
logins['factor'] = logins['factor'].str.replace('""', '')  
logins['device_type'] = logins['device_type'].str.replace('\\', '')
logins['device_type'] = logins['device_type'].str.replace('""', '') 
logins['result'] = logins['result'].str.replace('\\', '')
logins['result'] = logins['result'].str.replace('""', '')
logins['reason'] = logins['reason'].str.replace('\\', '')
logins['reason'] = logins['reason'].str.replace('""', '')
logins['type'] = logins['type'].str.replace('\\', '')
logins['type'] = logins['type'].str.replace('""', '')
logins['type'] = logins['type'].str.lower()

#service_trial_dict = dict(zip(services.service_id, services.trial))
#logins['service_trial'] = logins.service_id.map(service_trial_dict)
logins = logins[logins.active==True]

auth_mapping_dict = dict()
pay_string = ['zahlung','paiement','pagament','transaction','transfer','pay']
for i in pay_string:
    auth_mapping_dict[i] = 'pay'
auth_mapping_dict['card'] = 'card'
auth_mapping_dict['issuing3ds'] = 'pay'
auth_mapping_dict['pass'] = 'password or code'
auth_mapping_dict['login'] = 'login'
auth_mapping_dict['sign'] = 'login'
auth_mapping_dict['beneficiary'] = 'create_beneficiary'

print('Mapping new authentication type category from type column.')
logins['auth_type'] = 'other'
for k, v in auth_mapping_dict.items():
    logins['auth_type'] = np.where(logins['type'].str.contains(k), v, logins['auth_type'])

print('Write prepared data to file...')
#parqKey="prepared_data.parq.snappy"
#fp.write(parqKey, prod_logins ,compression='SNAPPY', open_with=myopen)

logins.to_parquet('prepared_data_2022.parquet.gzip',
              compression='gzip')

#chunks = np.array_split(prod_logins.index, 1000) # split into 1000 chunks

#for chunk, subset in enumerate(tqdm(chunks)):
#    if chunk == 0: # first row
#        prod_logins.loc[subset].to_csv('prepared_data_2022.csv', mode='w', index=True)
#    else:
#        prod_logins.loc[subset].to_csv('prepared_data_2022.csv', header=None, mode='a', index=True)
print('...Done.')

