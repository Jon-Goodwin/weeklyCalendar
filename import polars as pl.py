import polars as pl
import xlsxwriter as writer

df = pl.DataFrame({
    "A": [1, 2, 3, 2, 5],
    "B": ["x", "y", "x", "z", "y"]
})

with writer.Workbook('text_book.xlsx') as wb:
    # Create a new worksheet
    worksheet = wb.add_worksheet()
    # write the header for the calendar
    data_format1 = wb.add_format({'bg_color': '#FFC7CE'})
    data_format2 = wb.add_format({'bg_color': '#00C7CE'})
    #Write Polars data to the worksheet
    df.write_excel(wb, worksheet = 'Sheet1', autofilter= False,
                         autofit = True, position = 'A3', include_header = False ,
                         table_style=None, column_formats=None,conditional_formats=False)
    for row in range(0,10,2):
        worksheet.set_row(row+2, cell_format=data_format1)