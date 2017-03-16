# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 11:35:52 2016

@author: Matt Brady
"""

import pandas as pd
import numpy as np
import psycopg2 as pg
from psycopg2.extensions import register_adapter, AsIs
from optparse import OptionParser
from tia.bbg import LocalTerminal
from pandas import offsets as offsets
from pandas_datareader import data as pdr
import credentials


psql_creds = credentials.cred.psql_creds

def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.int64, adapt_numpy_int64)


def histDataReq(ticker,field,startTime,endTime,freq='DAILY',overrides=[]):

    data = {"ticker": ticker,
                "field": field,
                "startDate": startTime,
                "endDate": endTime,
                "periodicitySelection": freq,
                "overrides": overrides}
 
    for each in data["ticker"]:

        temp_df = LocalTerminal.get_historical(each,data["field"][0],start=data["startDate"],end=data["endDate"],period='DAILY')
        temp_df = temp_df.as_frame()
        temp_df = temp_df.swaplevel(0,1,axis=1)
        temp_df = temp_df.stack()

        temp_df.reset_index(inplace=True)
        temp_df = temp_df[['date',data['field'][0]]]

        temp_df.columns = ['date','mkt_value']

    return temp_df

def get_histData(tickers,fields,startTime,endTime,freq='DAILY',overrides=[]):
    df = pd.DataFrame()
    histData = {"securities": tickers,
                "fields": fields,
                "startDate": startTime,
                "endDate": endTime,
                "periodicitySelection": freq,
                "overrides": overrides}

    res = histDataReq(histData)
    df = parse_histDataReq(res)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    return df

def parse_histDataReq(res):
    dataDict = ast.literal_eval(res.read().decode('utf8').replace(":true",":True"))
    outdf = pd.DataFrame()
    tempdf = pd.DataFrame()
    for item in dataDict['data']:
        try:
            tempdf = pd.DataFrame(data = item['securityData']['fieldData'])
            tempdf['Ticker'] = item['securityData']['security']
            outdf = pd.concat([outdf,tempdf]).reset_index(drop=True)
            outdf['date'] = pd.to_datetime(outdf['date'])
        except Exception as e:
            e
            print(e)
            break
    return outdf


offset_dict = {
              "D":offsets.BDay,
              "W":offsets.Week,
              "M":offsets.MonthEnd,
              "Q":offsets.QuarterEnd
              }

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def update_data(creds,tckr_file):
    conn = pg.connect(**creds)
    curs = conn.cursor()

    freq_dict = {
        'D':'DAILY',
        'W':'WEEKLY',
        'M':'MONTHLY',
        'Y':'YEARLY'
    }

    tickers = pd.read_csv(tckr_file,parse_dates=[13,14])


    tickers['dt_end'] = tickers['dt_end'].map({'Inf':pd.datetime.today().date()-pd.offsets.BDay(1)})
    tickers['freq'] = tickers['freq'].map(freq_dict)

    tickers['dt_start'] = pd.Series(tickers['dt_start']).astype(str)
    tickers['dt_end'] = pd.Series(tickers['dt_end']).astype(str)


    column_count = len(tickers.columns)
    column_input = '%s' + (',%s' * (column_count - 5))
    column_array = list(tickers.columns)
    column_array = column_array[:-4]

    loop_str_columns = ""
    for each in column_array:
        loop_str_columns = loop_str_columns + each + ','


    loop_str_columns = loop_str_columns[:-1]

    for row_ind, row in tickers.iterrows():
        print(row['ticker'],row['field'])
        input_values_array = row.values.tolist()

        temp_array = []
        for each in input_values_array:
            if (pd.isnull(each)):
                each = None
            temp_array.append(each)

        input_values_array = tuple(temp_array[:-4])

        try:
            if row['data_source']=='BDH':
                if type(row['field'])!=str:
                    print('Field not specified for '+row['ticker'])
                    continue
                    row['freq'] = freq_dict[row['freq']]
                tmpdf = histDataReq([row['ticker']],[row['field']],row['dt_start'],row['dt_end'],freq=row['freq'])
            elif row['data_source'] =='FRED':
                offset_str = row['date_offset']
                n_offsets = int(offset_str[0])
                tmpdf = pdr.DataReader(row['ticker'],'fred',row['dt_start'] ,row['dt_end'] )
                tmpdf.reset_index(inplace=True)
                tmpdf.columns = ['date','mkt_value']
                if len(tmpdf)>0:
                    tmpdf = tmpdf.reset_index().dropna()
                    tmpdf['date'] = tmpdf['date']+offset_dict[offset_str[1]](n=n_offsets)

            if tmpdf.empty:
                print(row['dt_end'])
                print('Request returns no data for '+row['ticker'])
                continue

            else:


                if type(row['field'])!=str:
                    row['field'] = 'none'
                if type(row['date_offset'])!=str:
                    row['offset'] = 'none'

                curs.execute("""
                INSERT INTO tickers ({0})
                VALUES {1} ON CONFLICT (data_source, ticker,field) DO NOTHING;
                """.format(loop_str_columns, input_values_array))

                curs.execute("""
                    SELECT id FROM tickers WHERE data_source=%s AND ticker=%s AND field=%s;
                    """,[row['data_source'],row['ticker'], row['field']])

                ticker_id = curs.fetchone()[0]

                tmpdts = pd.DatetimeIndex(tmpdf['date']).to_pydatetime()
                tmpvals = tmpdf['mkt_value'].values
                n = len(tmpvals)
                record_list_template = ','.join(['%s']*n)
                args = list(zip([ticker_id]*n,tmpdts,tmpvals))

                #if mkt_number is a number or if a market text - would be the elif statment
                #No need for text so took it out and database doesn't have this field anyway yet.
                if is_number(str(tmpvals[0])):
                    curs.execute("""
                        INSERT INTO raw_data (ticker_id, mkt_date,mkt_value)
                        VALUES {0} ON CONFLICT (ticker_id, mkt_date, mkt_value) DO NOTHING;
                        """.format(record_list_template),args)
                else:
                    print("Error inserting; bad or no data")

        except AttributeError as e:
            print(e)
            continue

    curs.close()
    conn.commit()
    conn.close()
    return



            
            
ticker_file = 'SQL/ticker_input_strategy_meeting.csv'
update_data(psql_creds,ticker_file)
                