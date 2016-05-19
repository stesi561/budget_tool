#!/usr/bin/env python

# Requires
# https://pypi.python.org/pypi/xlrd

import xlrd
import sys
import psycopg2

config_file = "default.config"
inputfilename = "../data/b14-expenditure-data.xls"
datasheetname= 'Raw Data'
tablename = 'raw_data'

tnumeric = 'NUMERIC'
tvarchar = 'VARCHAR'
types = {0:None,1:tvarchar,2:tnumeric,3:'DATE',4:'BOOLEAN',5:'ERROR'}


def create_table(data,curr):
    # cols holds tuples (col_index_in_sheet,col_name,col_postgres_type,col_max_length)
    # col_max_length non zero for types of varchar
    curr.execute("drop table if exists %s CASCADE" % tablename)
    cols = []
    for index in range(data.ncols):        
        col_name = data.cell(0,index).value
        if "(" in col_name:
            col_name = col_name[:col_name.find("(")]
        col_name = col_name.strip()
        col_name = col_name.replace(" ", "_")

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
    ins_str = "CREATE TABLE %s (" %tablename
    ins_str += "tid" + " SERIAL"
    for c in cols:
        ins_str += ", "
        ins_str += c[1] + " " + c[2]
        if c[2] == tvarchar:
            ins_str+= "(%d)" % c[3]
    ins_str = ins_str + ");"
    curr.execute(ins_str)
    return cols




def load_data(data,cols, curr):
    ins_str = "INSERT INTO %s(" % tablename
    for col in cols:
        ins_str += "%s," % col[1]
    ins_str = ins_str[:-1] +") VALUES (" 
    for col in cols:
        ins_str += "%s,"
    ins_str = ins_str[:-1] + ")"

    input_data = []
    row = 1
    for row in range(1,data.nrows):
        input_data.append(data.row_values(row))

    print "Read Data"
    print "Loading into Postgresql"
    curr.executemany(ins_str,input_data)
    

def main(curr):
    book = xlrd.open_workbook(inputfilename)

    # Confirm presences of data sheet
    if datasheetname not in book.sheet_names():
        print "Data Sheet Name not found"
        print "Available sheets:" , book.sheet_names()
        sys.exit(1)
        
    data = book.sheet_by_name(datasheetname)
    cols = create_table(data,curr)
    load_data(data,cols,curr)
    

    

if __name__ == "__main__":

    #dbname='budget'
    #user='python'
    #host='localhost'
    #password='ReddEft7'
    
    
    #conn_str = "dbname='%s' user='%s' host='%s' password='%s'" % (dbname, user, host, password)

    with open(config_file,'r') as f:
        lines = f.readlines()

    conn_str = lines[0].rstrip('\n')
    
    conn = psycopg2.connect(conn_str)       
    curr = conn.cursor()
    main(curr)
    conn.commit()
        
        
        

