#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import csv

# Table Names
table = "raw_data"

# Column Names
primary_key = "tid"
department = "department"
vote = "vote"
year = "year"
amount = "amount"
app_id = "app_id"
appropriation_name = "appropriation_name"
category_name = "category_name"
functional_classification = "functional_classification"

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
            app_id,app_id, # match on these columns
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

def getIndex(colHeaders, value):
    index = 0
    while colHeaders[index] != value:
        index+= 1
    if colHeaders[index] != value:
        print "Failed to find %s in " % (value)
        print colHeaders
        sys.exit(1)
    return index    


class DictList:
    
    def __init__(self):
        self.lookup = dict()

    def add(self, key, value):
        if key not in self.lookup:
            self.lookup[key] = [value]
        else:
            self.lookup[key].append(value)

    def get(self, key):
        return self.lookup[key]

    def has(self, key):
        return key in self.lookup
    


def findUnique(curr):
    qry_str = """SELECT * FROM raw_data WHERE year = %s and vote='Environment'"""
    curr.execute(qry_str, (this_year,))
    dataCurrent = curr.fetchall()
    
    curr.execute(qry_str, (next_year,))
    dataNext = curr.fetchall()

    # Work from Next Budget and match from this budget
    # First set up a look up on App Id, Appropriation Name, Category Name and Functional Classification 
    # App Id is a unique ID but sometimes lines will change App ID
    # Appropriation Name, Category Name and Functional Classification are almost a unique key
    primary_key_dict = dict()
    app_id_dict = dict()
    category_name_dict = DictList()
    appropriation_name_dict = DictList()
    functional_classification_dict = DictList()
    
    colHeaders = [x[0] for x in curr.description]
    primary_key_index = getIndex(colHeaders,primary_key)
    app_id_index = getIndex(colHeaders,app_id)
    category_name_index = getIndex(colHeaders,category_name)
    appropriation_name_index = getIndex(colHeaders,appropriation_name)
    functional_classification_index = getIndex(colHeaders,functional_classification)

    rindex = 0
    category_names = []
    appropriation_names = []
    functional_classifications = []

    for row in dataCurrent:        
        app_id_dict[row[app_id_index]] = rindex            
        primary_key_dict[row[primary_key_index]] = rindex
        category_name_dict.add(row[category_name_index],rindex)
        appropriation_name_dict.add(row[appropriation_name_index],rindex)
        functional_classification_dict.add(row[functional_classification_index], rindex)
        rindex+= 1
    

    
    # Now iterate through dataNext matching in dataCurrent first on app id then
    # trying Appropriation Name, Category Name and Functional Classification on match record match
    #
    # For multiple possible matches record as a list and record in dups 
    
    dups = [] # primary key from dataNext
    matches = dict() # matches[dataNext.primarykey] = dataCurrent.primarykey (or list if dups)
    
    for row in dataNext:
        if row[app_id_index] in app_id_dict:
            index = app_id_dict[row[app_id_index]]
            if row[primary_key_index] in matches:
                if type(matches[row[primary_key_index]]) != type(['list']):
                    matches[row[primary_key_index]] = [matches[row[primary_key_index]]]
                matches[row[primary_key_index]].append(dataCurrent[index][primary_key_index])
                dups.append(row[primary_key_index])
            else:
                matches[row[primary_key_index]] = dataCurrent[index][primary_key_index]
        else:
            possible_rows = []
            # first look for match on category name
            if category_name_dict.has(row[category_name_index]):
                possible_rows = category_name_dict.get(row[category_name_index])                
                if len(possible_rows) == 1:
                    # SUCCESSS
                    matches[row[primary_key_index]] = dataCurrent[possible_rows[0]][primary_key_index]
                    continue
                # Two cases:
                # 1 len(possible_rows) > 0 - filter based on appropriation then functional classification
                # 2 len(possible_rows) == 0 - try another search based on appropriation then filter based on functional classification
                
                # Case 1:
                if len(possible_rows) == 0:
                    p = 0
                    while p < len(possible_rows):
                        # Find out if it doesn't match on Appropriation Name then Functional Classification
                        if (row[appropriation_name_index] != dataCurrent[possible_rows[p]][appropriation_name_index] or 
                            row[functional_classification_index] != dataCurrent[possible_rows[p]][functional_classification_index]):
                            del possible_rows[p]
                            # no need to inc as now p is index of next element in the list
                        else:
                            p+=1
                    if len(possible_rows) == 1:
                        matches[row[primary_key_index]] = dataCurrent[possible_rows[0]][primary_key_index]
                    else:
                        matches[row[primary_key_index]] = None
                else:
                    # Case 2
                    if appropriation_name_dict.has(row[appropriation_name_index]):
                        possible_rows = appropriation_name_dict.get(row[appropriation_name_index])                
                        if len(possible_rows) == 1:
                            # SUCCESSS
                            matches[row[primary_key_index]] = dataCurrent[possible_rows[0]][primary_key_index]
                            continue
                    if len(possible_rows) > 0:
                        p = 0
                        while p < len(possible_rows):
                            if row[functional_classification_index] != dataCurrent[possible_rows[p]][functional_classification_index]:
                                del possible_rows[p]
                            else:
                                p+= 1
                        if len(possible_rows) == 1:
                            matches[row[primary_key_index]] = dataCurrent[possible_rows[0]][primary_key_index]
                        else:
                            matches[row[primary_key_index]] = None

    with open("%s/%s.csv"% (directory,"temp.csv"),'w') as f:
        out = csv.writer(f) 
        out.writerow([primary_key+ " 2015", appropriation_name+ " 2015", category_name+ " 2015", functional_classification+ " 2015", app_id+ " 2015", "match type",
                      primary_key+ " 2014", appropriation_name+ " 2014", category_name+ " 2014", functional_classification+ " 2014", app_id+ " 2014"]
                 )
        for row in dataNext:            
            print "#################################"
            output = [row[primary_key_index], row[appropriation_name_index], row[category_name_index], row[functional_classification_index], row[app_id_index]]
            print row[primary_key_index]
            print matches[row[primary_key_index]]
            possibles =  matches[row[primary_key_index]]
            if possibles is None:
                output.append("None")                
            elif type(possibles) is type(['list']):
                output.append("Duplicates")
                for m in possibles:
                    row = dataLast[primary_key_dict[m]]
                    output += [row[primary_key_index], row[appropriation_name_index], row[category_name_index], row[functional_classification_index], row[app_id_index]]
                    output += "Next"
            else:
                output.append("Matches")
                row = dataCurrent[primary_key_dict[possibles]]
                output += [row[primary_key_index], row[appropriation_name_index], row[category_name_index], row[functional_classification_index], row[app_id_index]]                
            out.writerow(output)
        
        

def main(curr):
    findUnique(curr)
    
    
if __name__ == "__main__": 

    with open("default.config",'r') as f:
        conn_str = f.readline()                
        conn = psycopg2.connect(conn_str)       
        curr = conn.cursor()
        main(curr)
        conn.commit()


