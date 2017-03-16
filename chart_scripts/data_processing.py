import pandas as pd
import psycopg2 as pg
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
import signal
import datetime
from decimal import Decimal
import matplotlib.pyplot as plt
import scipy.stats as st
import time
import credentials

psql_creds = credentials.cred.psql_creds


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.int64, adapt_numpy_int64)

conn = None

def db_connect():

    global conn

    if not conn:
        conn = pg.connect(**psql_creds)
    return conn

def signal_handler(signal, frame):

    print("Process killed, closing connections")
    conn = get_connection()
    conn.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_db_data(field_filter):


    conn = db_connect()

    curs = conn.cursor()

    filter= True

    curs.execute("""
        SELECT id FROM tickers WHERE {}= {};
        """.format(field_filter,filter))

    id_tuple = curs.fetchall()

    id_list = [i[0] for i in id_tuple]

    #id_tuple = tuple(id_list)

    curs.execute("""
        SELECT ticker_id, short_name, mkt_date, mkt_value FROM data_ticker_id_join WHERE ticker_id = ANY(array{})
        """.format(id_list))

    data_tuple = curs.fetchall()

    temp_df = pd.DataFrame(list(data_tuple))

    temp_df.columns = ['ticker_id','short_name','mkt_date','mkt_value']

    curs.close()

    conn.commit()

    return temp_df

def upload_cumulative_tr(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()

    tmp_ticker_id = temp_df['ticker_id'].values
    tmpvals = temp_df['cumulative_return'].values
    tmp_short_name = temp_df['short_name'].values
    tmp_return_type = temp_df['return_type'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpdts, tmp_short_name, tmpvals,tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, mkt_date, short_name,cumulative_return,return_type)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_cumulative_tr_std(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()

    tmp_ticker_id = temp_df['ticker_id'].values
    tmpvals = temp_df['cumulative_std_return'].values
    tmp_short_name = temp_df['short_name'].values
    tmp_return_type = temp_df['std_type'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpdts, tmp_short_name, tmpvals,tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, mkt_date, short_name,cumulative_std_return,std_type)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_rolling_vol_std(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()

    tmp_ticker_id = temp_df['ticker_id'].values
    tmpvals = temp_df['vol_std'].values
    tmp_short_name = temp_df['short_name'].values
    tmp_return_type = temp_df['std_type'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpdts, tmp_short_name, tmpvals,tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, mkt_date, short_name,vol_std,std_type)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_rolling_vol(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()

    tmp_ticker_id = temp_df['ticker_id'].values
    tmpvals = temp_df['rolling_vol'].values
    tmp_short_name = temp_df['short_name'].values
    tmp_return_type = temp_df['std_type'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpdts, tmp_short_name, tmpvals,tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, mkt_date, short_name,rolling_vol,std_type)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_rolling_corr(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()


    tmpvals = temp_df['correlation'].values
    tmp_short_name_one = temp_df['short_name_one'].values
    tmp_short_name_two = temp_df['short_name_two'].values
    tmp_time_period = temp_df['period'].values

    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmpdts,tmpvals,tmp_short_name_one, tmp_short_name_two,tmp_time_period))

    curs.execute("""
        INSERT INTO {table_name} (mkt_date, correlation, short_name_one,short_name_two,time_period)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_rolling_corr_std(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()


    tmpvals = temp_df['correlation'].values
    tmp_short_name_one = temp_df['short_name_one'].values
    tmp_short_name_two = temp_df['short_name_two'].values
    tmp_time_period = temp_df['period'].values

    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmpdts,tmpvals,tmp_short_name_one, tmp_short_name_two,tmp_time_period))

    curs.execute("""
        INSERT INTO {table_name} (mkt_date, correlation_std, short_name_one,short_name_two,time_period)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_to_date_rt(temp_df,db_name):

    conn = db_connect()

    curs = conn.cursor()

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpd_short_names = temp_df.index.values
    tmpvals = temp_df['to_date_return'].values
    tmp_return_type = temp_df['return_type'].values
    tmp_ticker_id = temp_df['ticker_id'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpd_short_names, tmpvals, tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, short_name, to_date_return,return_type)
        VALUES {values};
        """.format(table_name=db_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def upload_trend_df(temp_df,db_name,col_name):

    conn = db_connect()

    curs = conn.cursor()

    temp_df.reset_index(inplace=True)

    sql = ('TRUNCATE ' + db_name)

    curs.execute(sql)

    tmpdts = pd.DatetimeIndex(temp_df['mkt_date']).to_pydatetime()

    tmp_ticker_id = temp_df['ticker_id'].values
    tmpvals = temp_df[col_name].values
    tmp_short_name = temp_df['short_name'].values
    tmp_return_type = temp_df['time_period'].values
    n = len(tmpvals)
    record_list_template = ','.join(['%s'] * n)
    args = list(zip(tmp_ticker_id,tmpdts, tmp_short_name, tmpvals,tmp_return_type))

    curs.execute("""
        INSERT INTO {table_name} (ticker_id, mkt_date, short_name,{name},time_period)
        VALUES {values};
        """.format(table_name=db_name,name=col_name,values=record_list_template), args)

    curs.close()

    conn.commit()

    return

def format_tr_index(tr_index_raw):

    temp_df = tr_index_raw.pivot(index='mkt_date', columns='short_name', values='mkt_value')

    temp_df.ffill(inplace=True)

    temp_df = temp_df.reindex(pd.date_range(temp_df.index[0] ,temp_df.index[-1],freq='D'),method='ffill')

    return temp_df

def get_date_array(data_df,period):

    ts_list = data_df.index.tolist()

    date_list = [ts.date() for ts in ts_list]

    last_date = date_list[-1]

    new_date_list = []

    for index in range(0,len(date_list),period):
        new_date_list.append((date_list[index]))

    new_date_list.append(last_date)

    return new_date_list

def cal_tr_views():

    field_filter = 'calc_returns'

    tr_index_raw = get_db_data(field_filter)

    tr_index_format = format_tr_index(tr_index_raw)

    tr_index_format.index = pd.to_datetime(tr_index_format.index )

    tr_index_format = tr_index_format.reindex(pd.date_range(tr_index_format.index[0], tr_index_format.index[-1], freq='D'),method='ffill')

    tr_index_format = tr_index_format.astype(float)

    output_df = tr_index_format.pct_change()

    return_period_dict = {30: 'One Month',90: 'Three Month',365:'One Year',1095:'Three Year',1825:'Five Year',2555: 'Seven Year'}

    loop_df = pd.DataFrame()

    std_loop = pd.DataFrame()

    for period, return_type in return_period_dict.items():

        date_array = get_date_array(output_df,period)

        rolling_total_return = tr_index_format / tr_index_format.shift(period) - 1

        std_df = (rolling_total_return - rolling_total_return.expanding(min_periods=period).mean()) / rolling_total_return.expanding(min_periods=period).std()

        #std_df = (std_df - std_df.rolling(window=2555).mean()) / std_df.rolling(window=2555).std()

        std_df = std_df.unstack().reset_index(name='cumulative_std_return').set_index('level_1')
        std_df['std_type'] = return_type

        std_loop = pd.concat([std_loop, std_df], axis=0)

        temp_df = output_df[-period:].cumsum()

        temp_df = temp_df.reindex(pd.date_range(temp_df.index[0] - datetime.timedelta(days=1), temp_df.index[-1], freq='D')).fillna(value=0)

        temp_df = temp_df.unstack().reset_index(name='cumulative_return').set_index('level_1')

        temp_df['return_type'] = return_type

        loop_df = pd.concat([loop_df,temp_df],axis=0)

    std_loop.index.names = ['mkt_date']

    std_loop.reset_index(inplace=True)

    tr_index_raw.drop_duplicates(subset=['ticker_id', 'short_name'], inplace=True)

    tr_index_raw.set_index('short_name', inplace=True)

    std_loop['ticker_id'] = std_loop.short_name.map(tr_index_raw.ticker_id)

    std_loop.dropna(axis=0,inplace=True)

    std_loop = std_loop[['ticker_id', 'mkt_date','short_name', 'cumulative_std_return', 'std_type']]

    loop_df.index.names = ['mkt_date']

    loop_df.reset_index(inplace=True)

    loop_df['ticker_id'] = loop_df.short_name.map(tr_index_raw.ticker_id)

    loop_df = loop_df[['ticker_id', 'mkt_date','short_name', 'cumulative_return', 'return_type']]

    upload_cumulative_tr(loop_df,db_name='cumulative_returns')

    upload_cumulative_tr_std(std_loop, db_name='cumulative_std_return')

    return

def cal_ann_to_date_views():

    field_filter = 'calc_returns'

    tr_index_raw = get_db_data(field_filter)

    tr_index_format = format_tr_index(tr_index_raw)

    return_period_dict = {7: 'One Week',30: 'One Month',90: 'Three Month',365:'One Year',1095:'Three Year',1825:'Five Year', 2555: 'Seven Year'}

    loop_df = pd.DataFrame()

    tr_index_format = tr_index_format.astype(float)

    for period, return_type in return_period_dict.items():

        if period > 365:
            annualize_factor = (365/period)
        else:
            annualize_factor = 1

        temp_df = (tr_index_format.iloc[-1].div(tr_index_format.iloc[-period]) ** annualize_factor - 1).to_frame().reset_index()

        temp_df['return_type'] = return_type

        loop_df = pd.concat([loop_df, temp_df], axis=0)

    loop_df.columns = ['short_name', 'to_date_return', 'return_type']

    tr_index_raw.drop_duplicates(subset=['ticker_id','short_name'],inplace=True)

    tr_index_raw.set_index('short_name',inplace=True)

    loop_df['ticker_id'] = loop_df.short_name.map(tr_index_raw.ticker_id)

    loop_df = loop_df[['ticker_id','short_name','to_date_return','return_type']]

    loop_df.set_index('short_name',inplace=True)

    upload_to_date_rt(loop_df,db_name='returns_to_date')

    return

def cal_rolling_volatility():

    field_filter = 'calc_returns'

    tr_index_raw = get_db_data(field_filter)

    tr_index_format = format_tr_index(tr_index_raw)

    tr_index_format.index = pd.to_datetime(tr_index_format.index )

    tr_index_format = tr_index_format.reindex(pd.date_range(tr_index_format.index[0], tr_index_format.index[-1], freq='D'),method='ffill')

    tr_index_format = tr_index_format.astype(float)

    output_df = tr_index_format.pct_change()

    return_period_dict = {30: 'One Month',90: 'Three Month',365:'One Year'}

    rolling_ann_vol_df_loop = pd.DataFrame()

    std_vol_df_loop = pd.DataFrame()

    for period, return_type in return_period_dict.items():

        rolling_ann_vol_df = output_df.ewm(halflife=period,min_periods=period).std() * np.sqrt(365)

        std_ann_vol_df = (rolling_ann_vol_df - rolling_ann_vol_df.expanding(min_periods=period).mean()) / rolling_ann_vol_df.expanding(min_periods=period).std()

        std_ann_vol_df = std_ann_vol_df.unstack().reset_index(name='vol_std').set_index('level_1')

        std_ann_vol_df['std_type'] = return_type

        std_vol_df_loop = pd.concat([std_vol_df_loop, std_ann_vol_df], axis=0)

        rolling_ann_vol_df = rolling_ann_vol_df.unstack().reset_index(name='rolling_vol').set_index('level_1')

        rolling_ann_vol_df['std_type'] = return_type

        rolling_ann_vol_df_loop = pd.concat([rolling_ann_vol_df_loop, rolling_ann_vol_df], axis=0)

    rolling_ann_vol_df_loop.index.names = ['mkt_date']

    rolling_ann_vol_df_loop.reset_index(inplace=True)

    std_vol_df_loop.index.names = ['mkt_date']

    std_vol_df_loop.reset_index(inplace=True)

    tr_index_raw.drop_duplicates(subset=['ticker_id', 'short_name'], inplace=True)

    tr_index_raw.set_index('short_name', inplace=True)

    std_vol_df_loop['ticker_id'] = std_vol_df_loop.short_name.map(tr_index_raw.ticker_id)

    std_vol_df_loop.dropna(axis=0, inplace=True)

    std_vol_df_loop = std_vol_df_loop[['ticker_id', 'mkt_date', 'short_name', 'vol_std', 'std_type']]

    rolling_ann_vol_df_loop['ticker_id'] = rolling_ann_vol_df_loop.short_name.map(tr_index_raw.ticker_id)

    rolling_ann_vol_df_loop.dropna(axis=0, inplace=True)

    rolling_ann_vol_df_loop.to_csv('test_erase.csv')
    rolling_ann_vol_df_loop = rolling_ann_vol_df_loop[['ticker_id', 'mkt_date', 'short_name', 'rolling_vol', 'std_type']]

    upload_rolling_vol_std(std_vol_df_loop,db_name='rolling_volatility_std')

    upload_rolling_vol(rolling_ann_vol_df_loop, db_name='rolling_volatility')

def cal_rolling_correlation():

    field_filter = 'calc_returns'

    tr_index_raw = get_db_data(field_filter)

    tr_index_format = format_tr_index(tr_index_raw)

    tr_index_format.index = pd.to_datetime(tr_index_format.index )

    tr_index_format = tr_index_format.reindex(pd.date_range(tr_index_format.index[0], tr_index_format.index[-1], freq='D'),method='ffill')

    tr_index_format = tr_index_format.astype(float)

    #tr_index_format = tr_index_format.ix[:,:3]

    tr_index_format_change = tr_index_format.pct_change()

    corr_dict = {90: 'Three Month', 365: 'One Year'}

    correlation_ts_df = pd.DataFrame()
    correlation_ts_std_df = pd.DataFrame()

    for period, period_text in corr_dict.items():
        print(period_text)

        #tr_index_df_corr = tr_index_format.rolling(window=period).corr(pairwise=True)

        tr_index_df_corr = tr_index_format.ewm(halflife=period, min_periods=period).corr()
        correlation_ts_df_loop = pd.DataFrame()

        correlation_ts_df_loop_std = pd.DataFrame()

        for key in tr_index_df_corr.minor_axis:

            loop_df = tr_index_df_corr.minor_xs(key)

            for index in loop_df.index.values:

                inner_loop = loop_df.loc[index:index]
                inner_loop_df = pd.DataFrame(inner_loop.values[0], index=inner_loop.columns.values,columns=['correlation'])

                inner_loop_std = inner_loop_df.expanding(min_periods=period).std().dropna()

                inner_loop_df.dropna(inplace=True)

                inner_loop_df['short_name_one'] = index
                inner_loop_df['short_name_two'] = key
                inner_loop_df['period'] = period_text

                inner_loop_df = inner_loop_df.reindex(pd.date_range(inner_loop_df.index[0], inner_loop_df.index[-1], freq='W'),method='ffill')

                inner_loop_std['short_name_one'] = index
                inner_loop_std['short_name_two'] = key
                inner_loop_std['period'] = period_text
                print(index)


                inner_loop_std = inner_loop_std.reindex(pd.date_range(inner_loop_std.index[0], inner_loop_std.index[-1], freq='W'),method='ffill')
                correlation_ts_df_loop = pd.concat([inner_loop_df,correlation_ts_df_loop],axis=0)

                correlation_ts_df_loop_std = pd.concat([inner_loop_std, correlation_ts_df_loop_std], axis=0)

        correlation_ts_df = pd.concat([correlation_ts_df_loop, correlation_ts_df], axis=0)

        correlation_ts_std_df = pd.concat([correlation_ts_df_loop_std, correlation_ts_std_df], axis=0)


    correlation_ts_df.reset_index(inplace=True)
    correlation_ts_df.rename(columns={'index': 'mkt_date'}, inplace=True)
    upload_rolling_corr(correlation_ts_df, db_name='rolling_corr')

    correlation_ts_std_df.reset_index(inplace=True)
    correlation_ts_std_df.rename(columns={'index': 'mkt_date'}, inplace=True)
    upload_rolling_corr_std(correlation_ts_std_df, db_name='rolling_corr_std')

    return


def cal_trends():


    field_filter = 'calc_returns'

    tr_index_raw = get_db_data(field_filter)

    tr_index_format = format_tr_index(tr_index_raw)

    tr_index_format_pct = tr_index_format.pct_change(periods=1)

    return_period_dict = {90: 'Three Month', 180: 'Six Month',365: 'One Year'}

    return_period_dict_tr = {90: 'Three Month', 180: 'Six Month',365: 'One Year',1095: 'Three Year',
                             1825: 'Five Year', 2555: 'Seven Year'}

    trend_df = pd.DataFrame()

    raw_trend = pd.DataFrame()

    rolling_tr = pd.DataFrame()

    tr_index_format = tr_index_format.astype(float)

    tr_index_raw.drop_duplicates(subset=['ticker_id', 'short_name'], inplace=True)

    tr_index_raw.set_index('short_name', inplace=True)

    for period, trend_type in return_period_dict_tr.items():

        rolling_tr_loop = tr_index_format / tr_index_format.shift(period) - 1


        rolling_tr_loop = rolling_tr_loop.unstack().reset_index(name='rolling_tr').set_index('level_1')

        rolling_tr_loop['time_period'] = trend_type

        rolling_tr = pd.concat([rolling_tr, rolling_tr_loop], axis=0)

    rolling_tr['ticker_id'] = rolling_tr.short_name.map(tr_index_raw.ticker_id)

    rolling_tr = rolling_tr[['ticker_id', 'short_name', 'rolling_tr', 'time_period']]

    rolling_tr.dropna(axis=0, inplace=True)

    rolling_tr.index.names = ['mkt_date']

    rolling_tr.reset_index(inplace=True)

    upload_trend_df(rolling_tr, db_name='rolling_tr', col_name='rolling_tr')

    for period, trend_type in return_period_dict.items():

        one_year_trend_df = (tr_index_format / tr_index_format.ewm(halflife=365,min_periods=365).mean()) - 1

        inner_loop_trend_df = (tr_index_format / tr_index_format.ewm(halflife=period, min_periods=period).mean()) - 1

        rolling_tr_loop = tr_index_format / tr_index_format.shift(period) - 1

        rolling_trend_std = (one_year_trend_df - rolling_tr_loop.expanding(min_periods=period*5).mean()) / rolling_tr_loop.expanding(min_periods=period*5).std()

        rolling_trend_std = rolling_trend_std.applymap(lambda x: (x * -1))

        inner_trend_change_prob = rolling_trend_std.apply(lambda x: st.norm.cdf(x))

        inner_trend_change_prob_of_return = inner_trend_change_prob.applymap(lambda x: x if (x <= 0.50) else (1 - x))

        trend_change_loop = inner_trend_change_prob_of_return.unstack().reset_index(name='prob_trend_change').set_index('level_1')

        trend_change_loop['time_period'] = trend_type

        trend_df = pd.concat([trend_df, trend_change_loop], axis=0)

        inner_loop_trend_df = inner_loop_trend_df.unstack().reset_index(name='rolling_trend').set_index('level_1')

        inner_loop_trend_df['time_period'] = trend_type

        raw_trend = pd.concat([raw_trend, inner_loop_trend_df], axis=0)


    trend_df['ticker_id'] = trend_df.short_name.map(tr_index_raw.ticker_id)

    trend_df = trend_df[['ticker_id', 'short_name', 'prob_trend_change', 'time_period']]

    trend_df.dropna(axis=0, inplace=True)

    trend_df.index.names = ['mkt_date']

    trend_df.reset_index(inplace=True)

    raw_trend['ticker_id'] = raw_trend.short_name.map(tr_index_raw.ticker_id)

    raw_trend = raw_trend[['ticker_id', 'short_name', 'rolling_trend', 'time_period']]

    raw_trend.dropna(axis=0, inplace=True)

    raw_trend.index.names = ['mkt_date']

    raw_trend.reset_index(inplace=True)

    upload_trend_df(trend_df, db_name='trend_probability', col_name='prob_trend_change')

    upload_trend_df(raw_trend, db_name='rolling_trend', col_name='rolling_trend')

    return

def main():

    start_time = time.time()
    cal_tr_views()
    cal_ann_to_date_views()
    cal_rolling_volatility()
    cal_rolling_correlation()
    cal_trends()
    #plt.show()
    print("--- %s seconds ---" % (time.time() - start_time))
main()

