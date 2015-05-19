#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import csv

# Table Names
table = "raw_data"
table2 = "raw_data"

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

        qry_str = "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s IN %s and %s = %s " % (
            app_id, vote, appropriation_name, category_name,appropriation_type, year, primary_key,scope,amount,
            table2,
            vote, '%s', year,'%s')
        curr.execute(qry_str, (votes_matching_to,this_year))
        lines_original = curr.fetchall()
        
        matches = match_lines(lines_next, lines_this)
        matches_original = match_lines(lines_next, lines_original)

        output_filename = "%s/byvote/%s.csv" % (directory, vote_matching)
        output_supps(matches, lines_next, lines_this, matches_original, lines_original, output_filename)

def match_lines(lines, lines_to_match):

    # Now work through lines_next matching from lines_this and marking the matchs
    # matches[lines_next_index] = lines_this_index
    matches = dict()

    # Create lookups on lines_this to reduce amount of searching
    lookup_this = None

    if len(lines_to_match) > 0:
        lookup_this = Lookup(lines_to_match, [vote, year])

        for line_num, line in zip(range(len(lines)),lines):
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
                if match_on_app_id(line, lines_to_match, lookup_this, matches,line_num):
                    print_match(line_num, line, lines_to_match, matches, 'app_id')
                    continue                                    
            if apppropriation_name_and_category_name_matching:
                if match_on_appropriation_name_and_category_name(line, lines_to_match, lookup_this, matches, line_num):
                    print_match(line_num, line, lines_to_match, matches, 'appropriation_name and category_name')
                    continue

            if appropriation_name_matching:
                if match_on_appropriation_name(line, lines_to_match, lookup_this, matches, line_num):
                    print_match(line_num, line, lines_to_match, matches, 'appropriation_name')
                    continue

            if category_name_matching:
                if match_on_category_name(line, lines_to_match, lookup_this, matches, line_num):
                    print_match(line_num, line, lines_to_match, matches, 'category name')                                    
                    continue

            if scope_matching:
                if match_on_scope(line, lines_to_match, lookup_this, matches, line_num):
                    print_match(line_num, line, lines_to_match, matches, 'category name')                                    
                    continue
    return matches

     
def output_supps(matches_adjusted, lines, lines_adjusted, matches_original, lines_original, output_file):
                    
    output = []
    matched = [set(),set()]
    for line_num, line in zip(range(len(lines)),lines):        
        line_out = [line,None,None]
        if line_num in matches_adjusted:                    
            line_out[1] = lines_adjusted[matches_adjusted[line_num]]
            matched[0].add(matches_adjusted[line_num])
        if line_num in matches_original:                    
            line_out[2] = lines_original[matches_original[line_num]]
            matched[1].add(matches_original[line_num])
        output.append(line_out)

    # Now match orginal lines - could optmise by only matching unmatched...
    matches = match_lines(lines_adjusted, lines_original)
    
    # and then append first lines_adjusted that were not matched with
    # a line from lines with any matching lines from lines_original
    # then append any unmatched lines from lines_original
    for line_num,line in zip(range(len(lines_adjusted)), lines_adjusted):
        if line_num in matched[0]:
            # line has been output already:
            continue
        line_out  = [None, line, None]
        if line_num in matches:
            line_out[2] = lines_original[matches[line_num]]
            matched[1].add(matches[line_num])
        output.append(line_out)
    
        for line_num in range(len(lines_original)):
            if line_num not in matched[1]:
                output.append([None, None, lines_original[line_num]])


    # Condense output list remove unnecessary data and add differences
    processing = output
    output = []
    for line in processing:
        output.append(output_line3(line[0],line[1],line[2]))

    output.sort()
            
    # Output files to directory/byvote/{{vote}}.csv
    with open(output_file, 'w')  as f:            
        csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL) 
        csvwriter.writerow([appropriation_name, category_name,appropriation_type,scope,
                            "%s Budget for" % this_year,"%s Est Actual" % this_year, "%s Budget for" % next_year,
                            '$ difference Est Act and Budget ', '% difference Est Act and Budget',
                            '$ difference Budget to Budget', '% difference Budget to Budget',])
                           
        for diff, row in output:
            csvwriter.writerow(row)
            

     
def output(matches, lines, lines_matched, output_file):
                    
    output = []
    for line_num, line in zip(range(len(lines)),lines):
        if line_num in matches:                    
            output.append(output_line(line,lines_matched[matches[line_num]]))
        else:
            output.append(output_line(line, None))

    # Output the lines from last year that were not matched
    matched = set(matches.values())
    for line_num in range(len(lines_matched)):
        if line_num not in matched:
            output.append(output_line(None,lines_matched[line_num]))

    output.sort()
    
            
    # Output files to directory/byvote/{{vote}}.csv
    with open(output_file, 'w')  as f:            
        csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)                            
        csvwriter.writerow([appropriation_name, category_name,appropriation_type,scope,this_year,this_year, next_year, '$ difference', '% difference'])
        for diff, row in output:
            csvwriter.writerow(row)
            
            
def output_line3(line,line_adjusted, line_original):
    columns = (appropriation_name, category_name,appropriation_type,scope)
    output = []
    getNamesFrom = line
    if line is None:
        if  line_adjusted is not None:
            getNamesFrom = line_adjusted
        else:
            getNamesFrom = line_original

    for col in columns:
        output.append(getNamesFrom[col])
    estimate = 0
    actual = 0
    last_estimate = 0
    if line is not None:
        estimate = line[amount]
    if line_adjusted is not None:
        actual = line_adjusted[amount]
    if line_original is not None:
        last_estimate = line_original[amount]

    output.append(last_estimate)    
    output.append(actual)
    output.append(estimate)

    output.append(estimate-actual)
    if actual > 0:
        output.append((estimate-actual)/(actual*1.0))
    else:
        output.append('')
    
    output.append(estimate-last_estimate)
    if last_estimate > 0:
        output.append((estimate-last_estimate)/(last_estimate*1.0))
    else:
        output.append('')

    sort = estimate-actual

    return (sort, output)

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


