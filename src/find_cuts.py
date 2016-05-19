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
appropriation_type = "appropriation_type"
scope = "current_scope"
amount = "amount"

# Years comparing
this_year = "2014"
next_year = "2015"


app_id_matching = True
apppropriation_name_and_category_name_matching = True
appropriation_name_matching = True
category_name_matching = True
scope_matching = True

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


class Lookup:
    
    def __init__(self,rows, exclude=[]):
        keys = rows[0].keys()
        self.lookups = dict()
        # For each key used in rows create a lookup identifying at which index it can be found.
        for key in keys:         
            if key in exclude:
                continue
            self.lookups[key] = dict()        
            for index,row in zip(range(len(rows)), rows):
                if row[key] not in self.lookups[key]:
                    self.lookups[key][row[key]] = []
                self.lookups[key][row[key]].append(index)
                

    def contains(self, keyname, key):
        return key in self.lookups[keyname]
    
    def get(self, keyname, key):
        if self.contains(keyname, key):
            return self.lookups[keyname][key]
        else:
            return []



    def printlookup(self, keyname):
        print self.lookups
        print self.lookups[keyname]
        
    
    def output(self, keyname):
        output=[]
        if keyname in self.lookups:
            for key in self.lookups[keyname]:
                output.append((key, self.lookups[keyname][key]))
        return output
            
    


def findUnique(curr, votes_merged, this_year,next_year):
    """Find matching budget lines - restricts searching to within votes - either of the same name or specified by votes_merged."""
    # votes_merged[year][vote] = [list of votes from year-1 which match vote]

    
    # Get list of votes
    qry_str = "SELECT %s FROM %s WHERE %s = %s GROUP by %s" % (vote,table, year, '%s', vote)
    curr.execute(qry_str, (next_year,))
    vote_names = curr.fetchall()
    
    all_votes = []

    # For each vote get all budget lines associated with it.
    # First get all from next_year then get this_years (which will need to include any from votes referenced in votes_merged)
    for vote_matching in vote_names:
        vote_matching = vote_matching[0]
        print vote_matching

        qry_str = "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s IN %s and %s = %s " % (app_id, vote, appropriation_name, category_name,appropriation_type, year, primary_key,scope,amount,
                                                                                         table,
                                                                                         vote, '%s', year,'%s')
        curr.execute(qry_str,((vote_matching,),next_year))
        lines_next = curr.fetchall()
        votes_matching_to = []
        if vote_matching in votes_merged[this_year]:
            votes_matching_to = votes_merged[this_year][vote_matching]
        votes_matching_to.append(vote_matching)
        votes_matching_to = tuple(votes_matching_to)
        curr.execute(qry_str,(votes_matching_to,this_year))
        lines_this = curr.fetchall()
        
        # Now work through lines_next matching from lines_this and marking the matchs
        # matches[lines_next_index] = lines_this_index
        matches = dict()

        # Create lookups on lines_this to reduce amount of searching
        lookup_this = None
        if len(lines_this) > 0:
            lookup_this = Lookup(lines_this, [vote, year])
        
            for line_num, line in zip(range(len(lines_next)),lines_next):
                # Order of priority for finding matches:
                # app_id            
                # appropriation_name and category_name
                # appropriation_name
                # category_name
                # scope
                #
                # Restrictions on matches
                # appropriation_type must be the same
                possibles = []
                if app_id_matching:            
                    if match_on_app_id(line, lines_this, lookup_this, matches,line_num):
                        print_match(line_num, line, lines_this, matches, 'app_id')
                        continue                                    
                if apppropriation_name_and_category_name_matching:
                    if match_on_appropriation_name_and_category_name(line, lines_this, lookup_this, matches, line_num):
                        print_match(line_num, line, lines_this, matches, 'appropriation_name and category_name')
                        continue

                if appropriation_name_matching:
                    if match_on_appropriation_name(line, lines_this, lookup_this, matches, line_num):
                        print_match(line_num, line, lines_this, matches, 'appropriation_name')
                        continue

                if category_name_matching:
                    if match_on_category_name(line, lines_this, lookup_this, matches, line_num):
                        print_match(line_num, line, lines_this, matches, 'category name')                                    
                        continue

                if scope_matching:
                    if match_on_scope(line, lines_this, lookup_this, matches, line_num):
                        print_match(line_num, line, lines_this, matches, 'category name')                                    
                        continue

            #print_unmatched(line)

        output = []
        for line_num, line in zip(range(len(lines_next)),lines_next):
            if line_num in matches:                    
                output.append(output_line(line,lines_this[matches[line_num]]))
            else:
                output.append(output_line(line, None))
            
        # Output the lines from last year that were not matched
        matched = set(matches.values())
        for line_num in range(len(lines_this)):
            if line_num not in matched:
                output.append(output_line(None,lines_this[line_num]))
                
                
        output.sort()

        all_votes.extend(output)
            
        # Output files to directory/byvote/{{vote}}.csv
        with open("%s/byvote/%s.csv" % (directory, vote_matching), 'w')  as f:            
            csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)                            
            csvwriter.writerow([appropriation_name, category_name,appropriation_type,scope,this_year, next_year, '$ difference', '% difference'])
            for diff, row in output:
                csvwriter.writerow(row)
    all_votes.sort()
    with open("%s/combined_%s_%s.csv" % (directory, next_year, this_year), 'w')  as f:            
        csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)                            
        for diff, row in all_votes:
            csvwriter.writerow(row)
            
def output_line(line_next, line_this):
    # Columns:
    # appropriation_name, category_name,appropriation_type,scope, $ this_year, $ next_year, $ difference, % difference )
    columns = (appropriation_name, category_name,appropriation_type,scope)
    output = []
    getNamesFrom = line_next
    if line_next is None:
        getNamesFrom = line_this
        line_next = dict()
        line_next[amount] = 0
    elif line_this is None:
        line_this = dict()
        line_this[amount] = 0

    for col in columns:
        output.append(getNamesFrom[col])
    this_year = line_this[amount]
    next_year = line_next[amount]

    output.append(this_year)
    output.append(next_year)
    output.append(next_year-this_year)
    if this_year > 0:
        output.append((next_year-this_year)/(this_year*1.0))
    else:
        output.append('')
    return (output[-2], output)

def match_on_single(line, lines_this, lookup_this, matches, line_num, match_on):
    possibles = lookup_this.get(match_on, line[match_on])
    if len(possibles) == 1:
        if confirm_match(line, lines_this[possibles[0]]):
            matches[line_num] = possibles[0]
            return True
    elif len(possibles) == 0:
        return False
    else:
        # Test all possibles against restriction criteria if only one
        # possible meets the criteria record it as a match.
        confirmed = []
        for possible_match in possibles:
            if confirm_match(line, lines_this[possible_match]):
                confirmed.append(possible_match)
        if len(confirmed) == 1:
            matches[line_num] = confirmed[0]
            return True
        else:
            return False

def match_on_scope(line, lines_this, lookup_this, matches, line_num):
    return match_on_single(line, lines_this, lookup_this, matches, line_num, scope)

def match_on_appropriation_name(line, lines_this, lookup_this, matches, line_num):
    return match_on_single(line, lines_this, lookup_this, matches, line_num, appropriation_name)

def match_on_category_name(line, lines_this, lookup_this, matches, line_num):
    return match_on_single(line, lines_this, lookup_this, matches, line_num, category_name)

def match_on_appropriation_name_and_category_name(line, lines_this, lookup_this, matches, line_num):
    possibles_a = set(lookup_this.get(appropriation_name, line[appropriation_name]))
    possibles_b = set(lookup_this.get(category_name, line[category_name]))
    possibles = list(possibles_a.intersection(possibles_b))
    
    if len(possibles) == 1:
        if confirm_match(line, lines_this[possibles[0]]):
            matches[line_num] = possibles[0]
            return True
    elif len(possibles) == 0:
        return False
    else:
        # Test all possibles against restriction criteria if only one
        # possible meets the criteria record it as a match.
        confirmed = []
        for possible_match in possibles:
            if confirm_match(line, lines_this[possible_match]):
                confirmed.append(possible_match)
        if len(confirmed) == 1:
            matches[line_num] = confirmed[0]
            return True
        else:
            return False


            
def match_on_app_id(line, lines_this, lookup_this, matches, line_num):
    possibles = lookup_this.get(app_id, line[app_id])
    if len(possibles) == 1:
        if confirm_match(line,lines_this[possibles[0]]):
            matches[line_num] = possibles[0]
            return True
        else:
            print "Failed to match. Same app_id, different appropriation_type"
            print line
            print lines_this[possibles[0]]
            sys.exit(1)
    elif len(possibles) > 1:
        print "Failed to match. Duplicate app id"
        sys.exit(1)
    else:
        return False

def confirm_match(line_next, line_this):
    return line_next[appropriation_type] == line_this[appropriation_type]

def print_unmatched(line):
    print "Not Matched"
    print line
    print
                    
def print_match(line_num, line, lines_this, matches, mtype = None):
    if True:
        return
    if mtype is not None:
        print mtype
    print line
    print lines_this[matches[line_num]]
    print 
    

def main(curr):
    votes_merged = dict()
    votes_merged['2012'] = dict()
    votes_merged['2012']['Environment'] = ['Climate Change']
    this_year = '2012'
    next_year = '2013'
    findUnique(curr, votes_merged, this_year, next_year)
    
    
if __name__ == "__main__": 

    with open("default.config",'r') as f:
        conn_str = f.readline()                
        conn = psycopg2.connect(conn_str)       
        curr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        main(curr)
        conn.commit()


