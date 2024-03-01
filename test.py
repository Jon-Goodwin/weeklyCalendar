import polars as pl
import xlsxwriter as writer
import datetime as dt

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
        
    return {k: calendar_dic[k] for k in my_order}
    
def extend_frames(reordered_dict: 'dict'):
    """Extends the dataframes with a null row for the purpose of the final xlsx formatting

    Args:
        reordered_dict (dict): the reordered calendar dictionary of key value 
    """
    null_row = [dt.datetime.today().strftime('%Y-%m-%d'),'','Event',
            'Month/mois', 'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    header = ['Canada','Country','Event','Month/mois',
               'Actual/Actuel', 'Forecast/Prevision', 'Previous/Precedant', 'Revised/Revise']
    new_row = pl.from_dict(dict(zip(header,null_row)))
    new_row = new_row.with_columns(pl.col("Canada").str.to_datetime("%Y-%m-%d")\
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
        new_calendar.extend(reordered_dict[val])
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
CaD_col= ['CANADA','','','Month / mois',
               'Actual / Actuel', 'Forecast / Prévision', 'Previous / Précédant', 'Revised / Révisé']
calendar = rename_calendar(calendar)

reordered_dict = partition_reorder(calendar)
index = index_list(reordered_dict)
extend_frames(reordered_dict)
new_calendar = recombine_calendar(reordered_dict)

with writer.Workbook('calendar_new.xlsx') as wb:
    # Create a new worksheet
    worksheet = wb.add_worksheet()
    # write the header for the calendar
    format3 = wb.add_format({'num_format': 'h:mm AM/PM','bg_color': '#333399',"font": "Arial",
                             'bold': True, 'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
    format4 = wb.add_format({'num_format': 'dd-mmm-yy','bg_color': '#333399',"font": "Arial",
                              'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
    format5 = wb.add_format({'bg_color': '#333399',"font": "Arial",
                              'font_color': '#FFFFFF','font_size': 12, 'align': 'center'})
    format_header = wb.add_format({ 'bg_color': '#333399', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
    bold_column = wb.add_format({'bold': True,"font": "Arial", 'font_size': 12})
    size_column = wb.add_format({'font_size': 12,"font": "Arial"})
    footer_format = wb.add_format({ 'bg_color': '#333399',"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
    format = wb.add_format({ 'bg_color': '#808080', 'bold': True,"font": "Arial",
                            'font_color': '#FFFFFF','font_size': 12})
    worksheet.write_row('A1', header, cell_format = format_header)
    #Write Polars data to the worksheet
    new_calendar.write_excel(wb, worksheet = 'Sheet1',
                         dtype_formats={pl.Date: "[$-en-US]d-mmm;@"}, autofilter= False,
                         autofit = True, position = 'A3', include_header = False, hide_gridlines= True,
                         column_formats= {"Actual/Actuel": {'align': 'center',
                                                            "bold": True, 'font_size': 12, "font": "Arial"},
                                          'Country': {'align': 'center',
                                                      'bold': True, 'font_size': 12, "font": "Arial"},
                                          'Event': {'align': 'center',
                                                    'font_size': 12, "font": "Arial"},
                                          'Canada': {'align': 'center',
                                                     'font_size': 12, "font": "Arial"},
                                          'Month/mois': {'align': 'center',
                                                         'font_size': 12,"font": "Arial"}, 
                                          'Forecast/Prevision': {'align': 'center',
                                                                 'font_size': 12,"font": "Arial"}, 
                                          'Previous/Precedant': {'align': 'center',
                                                                 'font_size': 12, "font": "Arial"}, 
                                          'Revised/Revise': {'align': 'center',
                                                             'font_size': 12,"font": "Arial"}})
    worksheet.write_row(1,0, data = CaD_col, cell_format = format)
    worksheet.write_row(index[0]+2,0,data = header3, cell_format = format)
    worksheet.write_row(index[0]+index[1]+3,0,data = header4, cell_format = format)
    worksheet.write_row(sum(index)+4, 0, data = footer1, cell_format = footer_format)
    worksheet.write_row(sum(index)+5, 0, data = footer2, cell_format = footer_format)
    worksheet.set_column(0,0, 15)
    worksheet.set_column(1,1, 10)
    worksheet.set_column(2,2, 40)
    worksheet.set_column(3,10, 25)
    worksheet.write_formula(0,7, '=NOW()', cell_format = format3)
    worksheet.write_formula(0,6, '=NOW()', cell_format = format4)
    worksheet.write_string(0,5, 'Updated:', cell_format = format5)