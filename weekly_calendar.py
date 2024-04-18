import polars as pl
import xlsxwriter as writer
import datetime as dt
import pandas as pd

# This script needs some cleanup still, if it breaks it's likely around new countries having relevance >= 50 or either
# Canada or the US not having data that week which would break the process.
# Also some functions contain hardcoded values could use some abstraction and could use some cleanup where the script
# builds the excel sheet

# Helper Functions

def partition_reorder(calendar: 'pl.dataframe.frame.Dataframe'):
    """Partitions the calendar frame by country into a dictionary and orders the dictionary

    Args:
        calendar (pl.dataframe.frame.Dataframe): _description_
    """


    calendar_dic = calendar.partition_by(by = 'Country', as_dict = True)

    my_order = ['CA', 'US', 'EC', 'FR', 'GE', 'IT', 'UK', 'JN', 'CH']
    new_countries = [x for x in list(calendar_dic.keys()) if x not in my_order]
    if new_countries:
        my_order.extend(new_countries)
        
    return {k: calendar_dic[k] for k in my_order if k in calendar_dic.keys()}
    
def extend_frames(reordered_dict: 'dict'):
    """Extends the dataframes with a null row for the purpose of the final xlsx formatting

    Args:
        reordered_dict (dict): the reordered calendar dictionary of key value 
    """
    null_row = [dt.datetime.today().strftime('%Y-%m-%d'),'','Event',
            'Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    header = ['CANADA','Country','Event','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
    new_row = pl.from_dict(dict(zip(header,null_row)))
    new_row = new_row.with_columns(pl.col("CANADA").str.to_datetime("%Y-%m-%d")\
            .cast(pl.Date))
                    
    reordered_dict['CA'].extend(new_row)
    reordered_dict['US'].extend(new_row)

def index_list(reordered_dict: 'dict'):
    """Creates a list of the length of entries in each sub countries calendar

    Args:
        reordered_dict (dict): a dictionary of key, value paires where keys are country codes
        and values are dataframes of events in that country
    """
    
    index_list = []
    for val in reordered_dict.values():
        index_list.append(val.select(pl.count()).item())
    return index_list

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

def recombine_calendar(reorded_dict):
    """Recbomines a dictionary of calendar slices into a single calendar

    Args:
        reorded_dict (_type_): A dictionary partitioned by country for keys and whose values
        are the corresponding dataframe
    """
    my_order = ['CA', 'US', 'EC', 'FR', 'GE', 'IT', 'UK', 'JN', 'CH'] #
    new_countries = [x for x in list(reordered_dict.keys()) if x not in my_order]
    if new_countries:
        my_order.extend(new_countries)
    new_calendar = reordered_dict[my_order[0]].clear()
    for val in my_order:
        if val in reordered_dict.keys():
            new_calendar.extend(reordered_dict[val])
        else:
            continue
    return new_calendar

def rename_calendar(calendar: 'pl.dataframe.frame.DataFrame'):
    """Renames the columns of the dataframe according to the desired style

    Args:
        calendar (pl.dataframe.frame.DataFrame): polars dataframe of country eco calendars
    """
    keys = calendar.columns
    values = ['CANADA','Country','Event','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
    
    ren = dict(zip(keys, values))
    return calendar.rename(ren)

def color_index_finder(calendar: 'pl.dataframe.frame.DataFrame') -> dict:
    """Takes in a partitioned dictionary of the calendars by country and outputs a list of indexes to be
    recolored in the xlsxwriter step

    Args:
        calendar (pl.dataframe.frame.DataFrame): _description_

    Returns:
        dict: a list lists containing integers corresponding to the dictionary they
    """
    new_dict = dict.fromkeys(['CA', 'US'],None)

    for k in new_dict.keys():
        dt_list = sorted(calendar[k].select(pl.col('CANADA')).unique().to_series().to_list())
        dt_list_odd = dt_list[1::2]

        row_numbers = calendar[k].with_columns(
        pl.when(pl.col("CANADA").is_in(dt_list_odd))
        .then(pl.arange(0, pl.col("CANADA").len()))
        .otherwise(None)
        .alias("row_numbers")
        )["row_numbers"].to_list()
        
        filtered_list = [item for item in row_numbers if item is not None]
        new_dict[k] = filtered_list
        
    return new_dict
    
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
calendar = calendar.with_columns(pl.col("Date Time").str.slice(0,10)).drop('Relevance') #some danger here
calendar = calendar.with_columns(pl.col("Date Time").str.strptime(pl.Date, "%m/%d/%Y", strict=False))

calendar = calendar.sort(['Country', 'Date Time'])

header = ['Economic Calendar of Events / Calendrier économique des événements','',
          '','','','Updated:', '=NOW()', '=NOW()']
header2 = ['CANADA','Country','Event','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
header3 = ['UNITED STATES/ETATS-UNIS','','','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
header4 = ['OTHER','','','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
footer1 = ['','','','', '', '', 'Briefing Line: 782-7000', '']
footer2 = ['','','','Pg 9', '', '', 'Rel. 2.8', '']
CaD_col = ['CANADA','','','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
calendar = rename_calendar(calendar)

reordered_dict = partition_reorder(calendar)

rows_color_CAUS = color_index_finder(reordered_dict)

index = dict(zip(reordered_dict.keys(),index_list(reordered_dict)))
index = list(index.values())
extend_frames(reordered_dict)
new_calendar = recombine_calendar(reordered_dict)
excel_base_date = pl.date('1900','1','1')
new_calendar = new_calendar.with_columns(
    pl.col("CANADA").cast(pl.Datetime).dt.timestamp("ms").truediv(86400000).add(25569).alias("CANADA"))

df1 = new_calendar.to_pandas()
df1.dtypes
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('new_calendar.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
df1.to_excel(writer, sheet_name='Sheet1', startrow= 2, header=False, index = False)

# Get the xlsxwriter objects from the dataframe writer object.
wb  = writer.book
worksheet = writer.sheets['Sheet1']

#Create formats
format3 = wb.add_format({'num_format': 'h:mm AM/PM','bg_color': '#333399',"font": "Arial",
                             'bold': True, 'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
format4 = wb.add_format({'num_format': 'dd-mmm-yy','bg_color': '#333399',"font": "Arial",
                              'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
format5 = wb.add_format({'bg_color': '#333399',"font": "Arial",
                              'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
date_column = wb.add_format({'num_format': '[$-en-US]d-mmm;@',"font": "Arial", 'font_size': 12})
date_column1 = wb.add_format({'num_format': '[$-en-US]d-mmm;@', 'bg_color': '#F2F2F2', "font": "Arial", 
                              'font_size': 12})
format_header = wb.add_format({ 'bg_color': '#333399', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
time_format1 = wb.add_format({'num_format': 'h:mm AM/PM','bg_color': '#333399', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
time_format2 = wb.add_format({'num_format': 'dd-mmm-yy','bg_color': '#333399', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
bold_column = wb.add_format({'bold': True,"font": "Arial", 'font_size': 12})
size_column = wb.add_format({'font_size': 12,"font": "Arial"})
footer_format = wb.add_format({ 'bg_color': '#333399',"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
format = wb.add_format({ 'bg_color': '#808080', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
data_format1 = wb.add_format({'bg_color': '#F2F2F2','font_size': 12,"font": "Arial"})

#write headers and footers
worksheet.write_row('A1', header, cell_format = format_header)
worksheet.write_formula('H1', '=NOW()', cell_format = time_format1)
worksheet.write_formula('G1', '=NOW()', cell_format = time_format2)
worksheet.write_row(1,0, data = CaD_col, cell_format = format)
worksheet.write_row(index[0]+2,0,data = header3, cell_format = format)
worksheet.write_row(index[0]+index[1]+3,0,data = header4, cell_format = format)
worksheet.write_row(sum(index)+4, 0, data = footer1, cell_format = footer_format)
worksheet.write_row(sum(index)+5, 0, data = footer2, cell_format = footer_format)

#set column widths
worksheet.set_column(0,0, 15, cell_format = date_column)
worksheet.set_column(1,1, 10, cell_format = bold_column)
worksheet.set_column(2,2, 40, cell_format = size_column)
worksheet.set_column(3,3, 15, cell_format = size_column)
worksheet.set_column(4,4, 25, cell_format = bold_column)
worksheet.set_column(5,10, 25, cell_format = size_column)

#set formulas in header
worksheet.write_formula(0,7, '=NOW()', cell_format = format3)
worksheet.write_formula(0,6, '=NOW()', cell_format = format4)
worksheet.write_string(0,5, 'Updated:', cell_format = format5)

#color rows
for row in rows_color_CAUS['CA']:
    worksheet.set_row(row+2, cell_format=data_format1)
    #when writing the conditional formatting index starts at 1, so we add 1
    worksheet.conditional_format(f'A{row+3}', {'type': 'no_errors',
                                          'format': date_column1})
for row in rows_color_CAUS['US']:
    worksheet.set_row(row+3+index[0], cell_format=data_format1)
    worksheet.conditional_format(f'A{row+4+index[0]}', {'type': 'no_errors',
                                          'format': date_column1})
#color bg of all 'Other' countries
worksheet.conditional_format('A1:A200', {'type': 'no_errors',
                                          'format': date_column})
for i in range(2,len(index),2):
    for row in range(sum(index[:i])+4,sum(index[:i+1])+4):
        worksheet.set_row(row, cell_format=data_format1)
        worksheet.conditional_format(f'A{sum(index[:i])+5}:A{sum(index[:i+1])+4}', {'type': 'no_errors',
                                          'format': date_column1})


#set conditional formatting
worksheet.conditional_format('E1:E200', {'type': 'no_errors',
                                          'format': bold_column})
worksheet.conditional_format('B1:B200', {'type': 'no_errors',
                                          'format': bold_column})

writer.close()

# Polars xlsxwriter engine sets a cell format for the dataframe making this code not usable until the engine changes

# with writer.Workbook('calendar_new.xlsx') as wb:
#     # Create a new worksheet
#     worksheet = wb.add_worksheet()
#     # write the header for the calendar
#     format3 = wb.add_format({'num_format': 'h:mm AM/PM','bg_color': '#333399',"font": "Arial",
#                              'bold': True, 'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
#     format4 = wb.add_format({'num_format': 'dd-mmm-yy','bg_color': '#333399',"font": "Arial",
#                               'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
#     format5 = wb.add_format({'bg_color': '#333399',"font": "Arial",
#                               'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
#     format_header = wb.add_format({ 'bg_color': '#333399', 'bold': True,"font": "Arial",
#                             'font_color': '#FFFFFF','font_size': 12})
#     bold_column = wb.add_format({'bold': True,"font": "Arial", 'font_size': 12})
#     size_column = wb.add_format({'font_size': 12,"font": "Arial"})
#     footer_format = wb.add_format({ 'bg_color': '#333399',"font": "Arial",
#                             'font_color': '#FFFFFF','font_size': 12})
#     format = wb.add_format({ 'bg_color': '#808080', 'bold': True,"font": "Arial",
#                             'font_color': '#FFFFFF','font_size': 12})
#     worksheet.write_row('A1', header, cell_format = format_header)
#     data_format1 = wb.add_format({'bg_color': '#FFC7CE'})
#     data_format2 = wb.add_format({'bg_color': '#00C7CE'})
#     #Write Polars data to the worksheet
#     new_calendar.write_excel(wb, worksheet = 'Sheet1',
#                          dtype_formats={pl.Date: "[$-en-US]d-mmm;@"}, autofilter= False,
#                          autofit = True, position = 'A3', include_header = False, hide_gridlines= True,
#                          column_formats= {"Actual/Actuel": {'align': 'center',
#                                                             "bold": True, 'font_size': 12, "font": "Arial"},
#                                           'Country': {'align': 'center',
#                                                       'bold': True, 'font_size': 12, "font": "Arial"},
#                                           'Event': {'align': 'center',
#                                                     'font_size': 12, "font": "Arial"},
#                                           'Canada': {'align': 'center',
#                                                      'font_size': 12, "font": "Arial"},
#                                           'Month/mois': {'align': 'center',
#                                                          'font_size': 12,"font": "Arial"}, 
#                                           'Forecast/Prevision': {'align': 'center',
#                                                                  'font_size': 12,"font": "Arial"}, 
#                                           'Previous/Precedant': {'align': 'center',
#                                                                  'font_size': 12, "font": "Arial"}, 
#                                           'Revised/Revise': {'align': 'center',
#                                                              'font_size': 12,"font": "Arial"}})
#     worksheet.write_row(1,0, data = CaD_col, cell_format = format)
#     worksheet.write_row(index[0]+2,0,data = header3, cell_format = format)
#     worksheet.write_row(index[0]+index[1]+3,0,data = header4, cell_format = format)
#     worksheet.write_row(sum(index)+4, 0, data = footer1, cell_format = footer_format)
#     worksheet.write_row(sum(index)+5, 0, data = footer2, cell_format = footer_format)
#     worksheet.set_column(0,0, 15)
#     worksheet.set_column(1,1, 10)
#     worksheet.set_column(2,2, 40)
#     worksheet.set_column(3,10, 25)
#     worksheet.write_formula(0,7, '=NOW()', cell_format = format3)
#     worksheet.write_formula(0,6, '=NOW()', cell_format = format4)
#     worksheet.write_string(0,5, 'Updated:', cell_format = format5)
#     for row in rows_color_CAUS['CA']:
#         worksheet.set_row(row+2, cell_format=data_format1)