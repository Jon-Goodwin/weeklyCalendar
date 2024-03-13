import polars as pl
import xlsxwriter as writer
import datetime as dt
import pandas as pd
    
    
df = {
   "date": [dt.date(2023, 11, 1), dt.date(2023, 11, 2), dt.date(2023, 11, 3),
            dt.date(2023, 11, 8), dt.date(2023, 11, 9)],
        "text": ["A", "B", "C", "D", "E"],
        "integer": [5, 12, 3, 6, 2]
    }
    
    
df = pl.DataFrame(df)
df = df.with_columns(
    pl.col("date").dt.strftime("%Y-%m-%d")
)
df1 = df.to_pandas()
    
# Create Pandas Excel Writer
writer = pd.ExcelWriter('test_book2.xlsx', engine='xlsxwriter')
    
# Convert to xlsx object
df1.to_excel(writer, sheet_name='Sheet1', startrow= 2, header=False, index = False)
    
# Get the xlsxwriter objects from the dataframe writer object.
wb  = writer.book
worksheet = writer.sheets['Sheet1']
#Create formats
data_format1 = wb.add_format({'bg_color': '#F2F2F2','font_size': 12,"font": "Arial"})
worksheet.set_row(2, cell_format=data_format1)
writer.close()