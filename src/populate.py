#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import sys

# Table Names
raw_data_table = "raw_data"

# Column Names
primary_key = "tid"
department = "department"
vote = "vote"
app_id = "app_id"
parent_id = "parent_id"
appropriation_name = "appropriation_name"
category_name = "category_name"
group_type = "group_type"
ismca = "ismca"
appropriation_type = "appropriation_type"
restriction_type = "restriction_type"
functional_classification = "functional_classification"
amount = "amount"
year = "year"
amount_type = "amount_type"
periodicity = "periodicity"
scope = "current_scope"




def create_table(column,curr):
    curr.execute("drop table if exists %s CASCADE" % column)
    qry_str = "SELECT ROW_NUMBER() OVER (ORDER BY %s) as id,  %s as name INTO %s from %s group by %s" % (column, column, column, raw_data_table,column)
    curr.execute(qry_str)



def set_up_tables(curr):
    # Create the easy tables
    for table in [department, vote, appropriation_type, functional_classification]:
        create_table(table,curr)
    
    # Create table to hold budget lines in.
    curr.execute("drop table if exists lines CASCADE")
    curr.execute("CREATE TABLE lines (lid SERIAL PRIMARY KEY, name varchar)");

    # Create table to match a line to a raw_data line
    curr.execute("drop table if exists matches CASCADE")
    curr.execute("CREATE TABLE matches (mid SERIAL PRIMARY KEY, lid integer REFERENCES lines(lid), tid integer REFERENCES raw_data(tid))");


    # Get the Most recent year and use to populate table lines with lines from that year.
    qry_str = "SELECT year FROM %s GROUP BY year ORDER BY year DESC LIMIT 1" % raw_data_table        
    curr.execute(qry_str)
    budget_year =  curr.fetchone()[0];

    qry_str = "SELECT tid, category_name FROM %s WHERE year = %s" % (raw_data_table, budget_year)
    curr.execute(qry_str)
    rows = curr.fetchall()
    lines = []
    ins_str = "INSERT INTO lines (name) VALUES (%s) RETURNING lid"
    for row in rows:
        curr.execute(ins_str, [row[1]])
        lid = curr.fetchone()[0]
        lines.append([lid, row[0]])

    run_test = False
    test_insert_lines(run_test, lines)


    ins_str = "INSERT INTO matches(lid, tid) VALUES (%s, %s)"
    curr.executemany(ins_str, lines)



def get_unmatched_qry_str(curr, select_col=None,where = None):
    """Returns an SQL query string to get all the raw data rows that are not matched"""
    if select_col is None:
        qry_str = "SELECT r.tid FROM "
    elif type(select_col) == type('string'):
        qry_str = "SELECT r.tid, r.%s FROM " % select_col
    elif type(select_col) == type(['list']):
        qry_str = "SELECT r.tid"
        for col in select_col:
            qry_str += ", r.%s" % column
        qry_str += " FROM "
    else:
        print "select_col arg get_unmatched_qry_str must be None, String or List. Passed :"
        print type(select_col)
        sys.exit(1)

    qry_str += "%s AS r LEFT JOIN matches AS m ON r.tid = m.tid WHERE m.tid IS NULL" % raw_data_table
    if type(where) is not type(dict()):
        print "where arg get_unmatched_qry_str must be Dict"
    for key in where:
        qry_str +=  " AND %s = %s" % (key, where[key])
        
    return qry_str

def find_matching_app_ids(curr):
    # Get app-ids of existing lines
    qry_str = "SELECT l.lid, m.tid, r.%s " % app_id
    qry_str += "FROM lines as l INNER JOIN matches AS M ON l.lid = m.lid "
    qry_str += "INNER JOIN %s AS r ON m.tid = r.tid" % raw_data_table
    curr.execute(qry_str)
    
    for r_line_id, r_raw_id, r_app_id in curr.fetchall():
        qry_str = "SELECT %s, tid from %s WHERE %s =" % (r_line_id, raw_data_table, app_id)
        qry_str += " %s"    
        curr.execute(qry_str,[r_app_id])
        matching_lines = curr.fetchall()
        ins_str = "INSERT INTO matches(lid, tid) VALUES (%s, %s)"
        curr.executemany(ins_str, matching_lines)

def show_matches(curr):

    # Each line holds a row that will be outputed in table form.
    # Key for lines is the lid.
    # Each row is a dict as well.
    lines = dict()
    
    # get years
    qry_str = "SELECT year FROM raw_data GROUP BY year ORDER BY year"
    curr.execute(qry_str)
    years = curr.fetchall()

    for year in years:
        qry_str = "SELECT l.lid,r.tid, r.amount FROM lines AS l INNER JOIN matches AS m ON l.lid = m.lid INNER JOIN raw_data as r ON m.tid = r.tid WHERE r.year = %s"
        curr.execute(qry_str, year)
        year = year[0] # remove from dictcursor row
        for row in curr.fetchall():
            lid = row['lid']
            if lid not in lines:
                lines[lid] = dict()
                for y in [x['year'] for x in years]:
                    lines[lid][y] = None
                    lines[lid]['tids'] = []
            if lines[lid][year] is not None:
                print "ERROR MULTIPLE matches for line.id %s in year %s was %s adding %s" % (lid, year, lines[lid][year], row['amount'])
                        
            lines[lid][year] = row['amount']
            lines[lid]['tids'].append(row['tid'])
            
    for row in lines:
        print lines[row]

def main(curr):
    #set_up_tables(curr)
    #find_matching_app_ids(curr)
    show_matches(curr)

def test_insert_lines(run, lines):
    if run:
        qry_stra = "SELECT name from lines where lid = %s"
        qry_strb = "SELECT category_name FROM raw_data where tid = %s"
        for row in lines:
            curr.execute(qry_stra, [row[0]])
            name = curr.fetchone()[0];
            curr.execute(qry_strb, [row[1]])
            cname = curr.fetchone()[0];
            if name != cname:
                print "Test Insert Lines failed at:"
                print row
                print name
                print cname
                sys.exit(1)
                


if __name__ == "__main__": 

    with open("default.config",'r') as f:
        conn_str = f.readline()                
        conn = psycopg2.connect(conn_str)       
        curr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        main(curr)
        conn.commit()
