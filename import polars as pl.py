import polars as pl
import xlsxwriter as writer
import pandas as pd

df = pl.DataFrame({
    "A": [1, 2, 3, 2, 5],
    "B": ["x", "y", "x", "z", "y"]
})

df1 = df.to_pandas()

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('test_book.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
df1.to_excel(writer, sheet_name='Sheet1', startrow= 2, header=False, index = False)

# Get the xlsxwriter objects from the dataframe writer object.
wb  = writer.book
worksheet = writer.sheets['Sheet1']
data_format1 = workbook.add_format({'bg_color': '#FFC7CE'})

worksheet.set_row(2, cell_format=data_format1)
writer.close()
