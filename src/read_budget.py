#!/usr/bin/env python

# Requires
# https://pypi.python.org/pypi/xlrd

import xlrd
import sys


inputfilename = "../data/b14-expenditure-data.xls"
datasheetname= 'Raw Data'

tnumeric = 'NUMERIC'
tvarchar = 'VARCHAR'
types = {0:None,1:tvarchar,2:tnumeric,3:'DATE',4:'BOOLEAN',5:'ERROR'}


def create_table(data):
    # cols holds tuples (col_index_in_sheet,col_name,col_postgres_type,col_max_length)
    # col_max_length non zero for types of varchar
    cols = []
    for index in range(data.ncols):        
        col_name = data.cell(0,index).value
        col_type = 0
        col_postgres_type = ""
        col_max_length = None
        row = 1
        while col_type == 0:
            col_type = data.cell(row,index).ctype
            row += 1
        # Check for max length
        if types[col_type] == tvarchar:
            col_max_length = max([len(str) for str in data.col_values(index,0,None)])+1
            col_postgres_type = types[col_type]

        # Check if is integer
        if types[col_type] == tnumeric:
            isInt = True
            row = 1
            while (isInt and row < data.nrows):
                isInt = (data.cell(row,index).value).is_integer()
                row+= 1
            
            if isInt:
                col_postgres_type = 'INTEGER'
            else:
                col_postgres_type = 'DOUBLE PRECISION'
        
        cols.append((index,col_name,col_postgres_type, col_max_length))
    ins_str = "CREATE TABLE tablename(\n\t"
    ins_str += "tid".ljust(25) + "\tSERIAL"
    for c in cols:
        ins_str += ",\n\t"
        ins_str += c[1].ljust(25) + "\t" + c[2]
        if c[2] == tvarchar:
            ins_str+= "(%d)" % c[3]
    ins_str = ins_str + "\n);"
    return ins_str
def main():
    book = xlrd.open_workbook(inputfilename)

    # Confirm presences of data sheet
    if datasheetname not in book.sheet_names():
        print "Data Sheet Name not found"
        print "Available sheets:" , book.sheet_names()
        sys.exit(1)
        
    data = book.sheet_by_name(datasheetname)
    print create_table(data)
        
    
    

if __name__ == "__main__":
    main()
