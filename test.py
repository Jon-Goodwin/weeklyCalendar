import pandas as pd
import polars as pl
import xlsxwriter as writer
import numpy as np
import datetime as dt

# Helper Functions
    
def country_list(calendar :'pl.dataframe.frame.DataFrame'):
    """Function takes a calendar polars dataframe with country column and reformats
    the column into ordered factor column.

    Args:
        calendar (polars.dataframe.frame.DataFrame): A polars dataframe with a 'Country'
        column of abbreviated country strings
        
    """
    # Create list of unique country values
    countries = calendar.select(['Country']).unique().to_series().to_list()
    # Set order of how countries will appear in the calendar
    my_order = ['CA', 'US', 'EC', 'FR', 'GE', 'IT', 'UK', 'JN', 'CH']
    # if a new country exists in the calendar, append it to the end
    new_countries = [x for x in countries if x not in my_order]
    if new_countries:
        my_order.extend(new_countries)
    # Convert the country column to an ordered factor using the order defined in 'my_order'    
    with pl.StringCache():
        pl.Series(my_order).cast(pl.Categorical)
        calendar=calendar.with_columns(pl.col('Country').cast(pl.Categorical))
    return calendar
    
def reshape_calendar(calendar : 'pl.dataframe.frame.DataFrame'):
    """
    Reshapes the given calendar by splitting into dictionary and then recombining with sub headers.

    Args:
        calendar (pl.dataframe.frame.DataFrame): a calendar which is a polars dataframe
    """
    d = calendar.partition_by(by = 'Country', as_dict = True)
    header = ['Economic Calendar of Events / Calendrier économique des événements','',
          '','','','Updated:', '=NOW()', '=NOW()']
    header2 = ['Canada','','','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    header3 = ['United States/ETATS-UNIS','','','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    header4 = ['Other','','','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    footer1 = ['','','','', '', '', 'Briefing Line: 782-7000', '']
    footer2 = ['','','','Pg 9', '', '', 'Rel. 2.8', '']
    
# import calendar
calendar_raw = pl.read_csv('bbg_cal.csv')
with pl.Config(tbl_cols = -1):
    print(calendar_raw)

# convert calendar export from bbg 
calendar = calendar_raw.select(pl.col('Date Time',calendar_raw.columns[1],\
    'Event',calendar_raw.columns[5], 'Survey','Actual', 'Prior', 'Revised', 'Relevance'))\
        .rename({calendar_raw.columns[1] : "Country",calendar_raw.columns[5] : 'Month'})\
        .filter(pl.col('Relevance') >= 50)\
        .sort(['Country', 'Date Time'])
# convert datetime string column to date format        
calendar = calendar.with_columns(pl.col("Date Time")\
    .str.to_datetime("%m/%d/%Y %H:%M")\
        .cast(pl.Date))\
        .drop('Relevance')

calendar = country_list(calendar)

calendar = calendar.sort(['Country', 'Date Time'])

header = ['Economic Calendar of Events / Calendrier économique des événements','',
          '','','','Updated:', '=NOW()', '=NOW()']
header2 = ['Canada','','\\','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
header3 = ['United States/ETATS-UNIS','','','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
header4 = ['Other','','','Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
footer1 = ['','','','', '', '', 'Briefing Line: 782-7000', '']
footer2 = ['','','','Pg 9', '', '', 'Rel. 2.8', '']

d  = calendar.partition_by(by = 'Country', as_dict = True)
keys = calendar.columns
values = header2
dict = dict(zip(keys, values))

calendar.rename(dict)


with writer.Workbook('calendar_new.xlsx') as wb:
    # Create a new worksheet
    worksheet = wb.add_worksheet()
    # write the header for the calendar
    worksheet.write_row('A1', header)
    worksheet.write_row('A2',header2)
    #Write Polars data to the worksheet
    calendar.write_excel(wb, worksheet = 'Sheet1', dtype_formats={pl.Date: "[$-en-US]d-mmm;@"}, autofit = True, position = 'A3')