#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import csv

# Table Names
table = "raw_data"

# Column Names
department = "department"
vote = "vote"
year = "year"
amount = "amount"
appid = "app_id"
appropriation_name = "appropriation_name"
category_name = "category_name"

# Years comparing
this_year = "2014"
next_year = "2015"


# Output directory
directory = "../output"

def byvote(curr):
    qry_str = "SELECT %s from %s group by %s" % (vote, table, vote)
    curr.execute(qry_str)
    votes =  [x[0] for x in curr.fetchall()]        
    for vote_outputing in votes:
        # Find any cuts made on lines with the same app id
        qry_str = "SELECT thisyr.%s as dept,thisyr.%s as app_name ,thisyr.%s as cat_name ,thisyr.%s as thisyramt, nextyr.%s as nextyramt, nextyr.%s-thisyr.%s as diff FROM %s as thisyr INNER JOIN %s as nextyr ON thisyr.%s = nextyr.%s  WHERE thisyr.%s = #! and nextyr.%s = #! and thisyr.%s = #! order by diff DESC" % (
            vote, appropriation_name, category_name,amount,amount,amount, amount, # output variables
            table, # this year
            table, # next year
            appid,appid, # match on these columns
            year, # Restrict to only this year
            year, # Restrict to only next year
            vote  # limit to vote
        )
        qry_str = qry_str.replace('#!', '%s')

        curr.execute(qry_str, (this_year, next_year,vote_outputing))
        with open("%s/byvote/%s.csv"% (directory,vote_outputing),'w') as f:
            out = csv.writer(f)
            out.writerow([x[0] for x in curr.description])
            for row in curr.fetchall():
                out.writerow(row)


def main(curr):
            
    
if __name__ == "__main__":
    with open("default.config",'r') as f:
        conn_str = f.readline()                
        conn = psycopg2.connect(conn_str)       
        curr = conn.cursor()
        main(curr)
        conn.commit()


