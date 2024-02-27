import pandas as pd
import polars as pl
import numpy as np
import datetime as dt


# import calendar
calendar_raw = pl.read_csv('bbg_cal.csv')
with pl.Config(tbl_cols = -1):
    print(calendar_raw)

# 


calendar = calendar_raw.select(pl.col('Date Time',calendar_raw.columns[1],\
    'Event',calendar_raw.columns[5], 'Survey','Actual', 'Prior', 'Revised', 'Relevance'))\
        .rename({calendar_raw.columns[1] : "Country",calendar_raw.columns[5] : 'Month'})\
        .filter(pl.col('Relevance') >= 50)\
        .sort(['Country', 'Date Time'])
        
calendar.with_columns(pl.col("Date Time")\
    .str.to_datetime("%m/%d/%Y %H:%M")\
        .cast(pl.Date))

pl.read_excel('calendar.xlsm', sheet_name = None)