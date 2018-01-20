###############################################################################################################
# This code aims to help update the database by saving old database files into a new one
#
###############################################################################################################

# open the sqlite and set the connection on the database
import sqlite3

# in sqlite the only option to convert table is to rename temporary table, create new table, and then drop old table

#SCHEMA FOR THE RELEVANT TABLES

querycreate = {}

querycreate['urltable']='''CREATE TABLE IF NOT EXISTS urltable 
	(uniqueid INTEGER,
	urls VARCHAR(200),
	urlpostdatetime VARCHAR(100),
	postdate DATE,
	PRIMARY KEY(uniqueid,postdate));'''
	
querycreate['pagedata']='''CREATE TABLE IF NOT EXISTS pagedata (
		uniqueid INTEGER,
		postdate DATE,
		posttime VARCHAR(5),
		downloaddate DATE,
		downloadtime VARCHAR(5),
		stat VARCHAR(10),
		jobtitle VARCHAR(50),
		company VARCHAR(50),
		location VARCHAR(50),
		num_applicants INTEGER,
		num_vacancies INTEGER,
		num_seen INTEGER,
		num_shortlisted INTEGER,
		num_rejected INTEGER,
		experience_needed VARCHAR(50),
		career_level VARCHAR(50),
		job_type VARCHAR(50),
		salary VARCHAR(50),
		education_level VARCHAR(50),
		gender VARCHAR(10),
		travel_frequency VARCHAR(20),
		languages VARCHAR(30),
		vacancies VARCHAR(15),
		roles VARCHAR(300),
		keywords VARCHAR(100),
		requirements VARCHAR(5000),
		industries VARCHAR(100),
		PRIMARY KEY(uniqueid,postdate,downloaddate));
		'''

querycreate['archivedpagedata'] = '''CREATE TABLE IF NOT EXISTS archivedpagedata (
		uniqueid INTEGER,
		postdate DATE,
		posttime VARCHAR(5),
		downloaddate DATE,
		downloadtime VARCHAR(5),
		stat VARCHAR(10),
		jobtitle VARCHAR(50),
		company VARCHAR(50),
		location VARCHAR(50),
		num_applicants INTEGER,
		num_vacancies INTEGER,
		num_seen INTEGER,
		num_shortlisted INTEGER,
		num_rejected INTEGER,
		experience_needed VARCHAR(50),
		career_level VARCHAR(50),
		job_type VARCHAR(50),
		salary VARCHAR(50),
		education_level VARCHAR(50),
		gender VARCHAR(10),
		travel_frequency VARCHAR(20),
		languages VARCHAR(30),
		vacancies VARCHAR(15),
		roles VARCHAR(300),
		keywords VARCHAR(100),
		requirements VARCHAR(5000),
		industries VARCHAR(100),
		PRIMARY KEY(uniqueid,postdate,downloaddate));
		'''
	
tables = ['urltable','pagedata','archivedpagedata']
    
#Report standard statistics and counts of the data that is in each of the tables    
def report_statistics():
    
	conn = sqlite3.connect("wuzzuf.db")
	c = conn.cursor()
    
    for i, t in enumerate(tables):
        query ='''SELECT * FROM {};'''
        temp = c.execute(query.format(t)).fetchall()
        print("Number of observations in table {}: {}".format(t,len(temp)))        
        query ='''PRAGMA table_info({});'''
        schema = c.execute(query.format(t)).fetchall()
        print(schema)
        
        if t == "urltable":
            query = '''SELECT MAX(DATE(postdate)) FROM {};'''.format(t)
        else:
            query = '''SELECT MAX(DATE(downloaddate)) FROM {};'''.format(t)
        
        print(c.execute(query).fetchall())
        lastdate = c.execute(query).fetchall()[0][0]
        print("Last date to query: {}".format(lastdate))
        
        #Check the most recent data and report counts, means for each of the data points in the dataset
        for j, row in enumerate(schema):
            var = row[1]
            if t == "jobadpageurls":
                querystats = '''SELECT DATE(postdate), AVG({}), COUNT({}) FROM {} GROUP BY DATE(postdate);'''.format(var,var,t,lastdate)
            else:
                querystats = '''SELECT DATE(downloaddate), AVG({}), COUNT({}) FROM {} GROUP BY DATE(downloaddate);'''.format(var,var,t,lastdate)
            print(querystats)
            queryresults = {}
            print("{}: {}".format(var,c.execute(querystats).fetchall()))
        
        print("\n")

    c.close()
	
#function resets all of the tables
def reset_tables():

    for i, t in enumerate(tables):
        deletequery = '''DROP TABLE IF EXISTS {};'''
        c.execute(deletequery.format(table[i]))
        c.execute(querycreate[t])
        query ='''PRAGMA table_info({});'''
        print(c.execute(query.format(table[i])).fetchall())
    conn.commit()
    conn.close()
	
def update_table(tablename,newtablequery):
	
	conn = sqlite3.connect("wuzzuf.db")
	c = conn.cursor()
	
	query = "ALTER TABLE {} RENAME TO {}_temp;"
	c.execute(query.format(tablename,tablename))
	
	c.execute(newtablequery)
	
	query = "INSERT INTO {} SELECT * FROM {}_temp;"
	c.execute(query.format(tablename,tablename))
	query ='''PRAGMA table_info({});'''
	print(c.execute(query.format(tablename)).fetchall())
	
	query = '''DROP TABLE IF EXISTS {}_temp;'''
	c.execute(query.format(tablename))
	
	temp = c.execute('''SELECT * FROM {};'''.format(tablename)).fetchall()
	print("Number of entries in converted table {}: {}".format(tablename,len(temp)))
	
	conn.commit()
	conn.close()

# Comment or uncomment as needed
reset_tables()
# report_statistics()
# update_table()   
