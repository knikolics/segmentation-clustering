import pandas as pd
import numpy as np
import glob
import os
import sys

import datetime
from tqdm import tqdm

import pyarrow as pa
import pyarrow.csv as csv

import math
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')


def get_top_companies(df, top_n=10):
    top_orgs = []
    top_orgs = df.groupby('organisation')['authentications'].sum().sort_values(ascending=False).iloc[:top_n].index.tolist()
    return top_orgs


# define quantile functions: 5th, 10th, 25th, 50th, 75, 90
def q05(x):
    return x.quantile(0.05)

def q10(x):
    return x.quantile(0.1)

def q25(x):
    return x.quantile(0.25)

def q50(x):
    return x.quantile(0.5)

def q75(x):
    return x.quantile(0.75)

def q90(x):
    return x.quantile(0.9)

def calc_mode(x):
    return x.mode()

def calc_ts_features_per_month(grouping_unit,df):
     if grouping_unit=='organisation':
           grouping_list = [grouping_unit, 'month']
     else:
           grouping_list = [grouping_unit,'organisation', 'month']
     """Describe duration distribution within windows."""
     dist = df.groupby(grouping_list).agg(
            ts_Std=('authentications', 'std'),
            ts_Mean=('authentications', 'mean'),
            ts_Sum=('authentications', 'sum'),
            ts_Min=('authentications', 'min'),
            ts_Max=('authentications', 'max'),
            ts_range = ('authentications', lambda x: x.max() - x.min()),
            ts_05=('authentications', q05),
            ts_10=('authentications', q10),
            ts_25=('authentications', q25),
            ts_50=('authentications', q50),
            ts_75=('authentications', q75),
            ts_90=('authentications', q90)).reset_index()
            
     data_grouped = dist[dist.ts_Std.notna() & dist.ts_Mean.notna()]
     return data_grouped 

# groupby with variable step and window size, unit = days
def calc_ts_features_variable_timespan(stepsize=15, windowsize=30): 
    users = data.reset_index().user_id.unique().tolist()
    date_format = '%Y-%m-%d'
    full_df = pd.DataFrame()

    for usr in users:
        udf = df[df.index.get_level_values(0)==usr].reset_index()
        dates = sorted(udf.day.astype(str).unique().tolist())
        #print(dates[0])
        start = datetime.datetime.strptime(dates[0], date_format).date().replace(day=1)
        end = datetime.datetime.strptime(dates[-1], date_format).date()  + relativedelta(day=31)
        timespan = pd.date_range(start, end, freq="{}D".format(stepsize))
        org = udf.organisation.unique().tolist()[0]
        full_usr_df = pd.DataFrame()
        udf['day'] = pd.to_datetime(udf['day'].astype(str),format=date_format)

        for idx, time in enumerate(timespan):
            usr_df = pd.DataFrame(index=[idx],columns=['user_id','organisation','start_date','end_date',
                                       'ts_Std','ts_Mean','ts_Sum','ts_Min','ts_Max','ts_range',
                                       'ts_05','ts_10','ts_25','ts_50','ts_75','ts_90'])
            start_date = time.date()
            end_date = time.date() + datetime.timedelta(days=windowsize)
            tmp = udf[(udf.day.dt.date >= start_date) & (udf.day.dt.date < end_date)]
            #print(tmp)
            usr_df['user_id'] = usr
            usr_df['organisation'] = org
            usr_df['start_date'] = start_date
            usr_df['end_date'] = end_date
            usr_df['ts_Std'] = tmp.authentications.std()
            usr_df['ts_Mean'] = tmp.authentications.mean()
            usr_df['ts_Sum'] = tmp.authentications.sum()
            usr_df['ts_Min'] = tmp.authentications.min()
            usr_df['ts_Max'] = tmp.authentications.max()
            usr_df['ts_range'] = tmp.authentications.max() - tmp.authentications.min()
            usr_df['ts_05']= tmp.authentications.quantile(0.05)
            usr_df['ts_10']= tmp.authentications.quantile(0.1)
            usr_df['ts_25']= tmp.authentications.quantile(0.25)
            usr_df['ts_50']= tmp.authentications.quantile(0.5)
            usr_df['ts_75']= tmp.authentications.quantile(0.75)
            usr_df['ts_90']= tmp.authentications.quantile(0.9)
            full_usr_df = pd.concat([full_usr_df, usr_df], axis=0)
            #print(usr_df)
        full_df = pd.concat([full_df, full_usr_df], axis=0)
    full_df = full_df[full_df.ts_Mean.notna()]
    return full_df
    


if __name__ == "__main__":

   try:
       print('\nReading parquet prepared data file.\n')
       data = pd.read_parquet('prepared_data_2022.parquet.gzip')
   except:
       print('prepared_data.parquet.gzip not found. --> run first data_prep-py')
       sys.exit()

   #remove one time users
   otu_df = data.user_id.value_counts().to_frame()
   otu = otu_df[otu_df.user_id==1].index.tolist()
   data = data[~data.user_id.isin(otu)] 

   index_level=0 # default is 0, will be set automatically however by defining grouping_unit; 0 - user, 1 - organisation
   cutoff = '2020-12'
   grouping_unit = 'user_id' # user_id or organisation
   # define top n organisations below (top_n)
   

   if grouping_unit == 'user_id':
       cutoff = '2021-12'
       #orgs = get_top_companies(data, top_n=5)
       #print('Processing only following organisations:', orgs)
       index_level = 0
       data = data[(data.month > cutoff)]# & (data.organisation.isin(orgs))]
   else:
       index_level = 1
       data = data[(data.month > cutoff)]

   mode = 'fixed' # fixed (1month) or variable (define window and step size
   df = data.groupby(['user_id','organisation','month','week','day'])['authentications'].sum().to_frame()
   print(df.head(3))
#   df = pa.TableGroupBy(data,['user_id','organisation','month','week','day']).aggregate([('authentications','sum')])

   if mode == 'fixed':
       print('******************************************')
       print('*         1 month window/step size       *')
       print('*         group by:{}     *'.format(grouping_unit)) 
       print('******************************************')
       chunks = np.array_split(df.reset_index()[grouping_unit].unique().tolist(), 100)
       #print(chunks)
       
       data_set = pd.DataFrame()
       for chunk, subset in enumerate(tqdm(chunks)):
             tmp_data_set = calc_ts_features_per_month(grouping_unit,df[df.index.get_level_values(index_level).isin(subset)])
             data_set = pd.concat([data_set, tmp_data_set], axis=0)
   else:
       print('******************************************')
       print('*      30/15 days window/step size       *')
       print('*         group by:{}     *'.format(grouping_unit)) 
       print('******************************************')
       data_set = calc_ts_features_variable_timespan(stepsize=15, windowsize=30) 
   print(data_set.head(5))
   del df

   print('Saving to parquet...\n')
   data_set.to_parquet('time_features_data_monthly_{}_{}_after_{}_top5.parquet.gzip'.format(mode,grouping_unit,cutoff),
              compression='gzip')

   print('File time_features_data_monthly_{}_{}_after_{}_top5.parquet.gzip written'.format(mode,grouping_unit,cutoff))
