###############################################################################################################
# This code aims to help create and update the database by saving old database files into a new one
#
###############################################################################################################

# open the sqlite and set the connection on the database
# open the sqlite and set the connection on the database
import sqlite3

# in sqlite the only option to convert table is to rename temporary table, create new table, and then drop old 

#SCHEMA FOR THE RELEVANT TABLES

querycreate = {}

querycreate['regionadcounts'] = '''CREATE TABLE IF NOT EXISTS regionadcounts (
    downloaddate DATE,
    downloadtime VARCHAR(5),
    region VARCHAR(50),
    freg VARCHAR(50),
    subregion VARCHAR(50),
    fsubreg VARCHAR(50),
    totalregposts INTEGER,
    subposts INTEGER,
    PRIMARY KEY(downloaddate,region,subregion));'''
    
querycreate['regionjobadcounts']= '''CREATE TABLE IF NOT EXISTS regionjobadcounts 
    (downloaddate DATE,
    downloadtime VARCHAR(5),
    region VARCHAR(50),
    freg VARCHAR(50),
    subregion VARCHAR(50),
    fsubreg VARCHAR(50),
    sector VARCHAR(50),
    urlregsector VARCHAR(50),
    totalposts INTEGER,
    PRIMARY KEY(downloaddate,region,subregion,sector));'''
    
querycreate['jobadpageurls'] = '''CREATE TABLE IF NOT EXISTS jobadpageurls (
        region VARCHAR(50),
        freg VARCHAR(50),
        subregion VARCHAR(50),
        fsubreg VARCHAR(50),
        jobsector VARCHAR(50),
        postdate DATE,
        uniqueadid INTEGER,
        i_photo INTEGER,
        i_featured INTEGER,
        urllinkshort VARCHAR(50),
        PRIMARY KEY(uniqueadid,postdate));'''

querycreate['jobadpagedata'] = '''CREATE TABLE IF NOT EXISTS jobadpagedata (
        downloaddate DATE,
        downloadtime VARCHAR(5),
        region VARCHAR(50),
        freg VARCHAR(50),
        subregion VARCHAR(50),
        fsubreg VARCHAR(50),
        jobsector VARCHAR(50),
        uniqueadid INTEGER,
        postdate DATE,
        posttime VARCHAR(5),
        pageviews INTEGER,
        title VARCHAR(70),
        experiencelevel VARCHAR(50),
        educationlevel VARCHAR(50),
        type VARCHAR(50),
        employtype VARCHAR(50),
        compensation INTEGER,
        description VARCHAR(5000),
        textlanguage VARCHAR(2),
        userhref VARCHAR(30),
        username VARCHAR(50),
        userjoinyear INTEGER,
        userjoinmt VARCHAR(3),
        emailavail INTEGER,
        phoneavail INTEGER,
        adstatus VARCHAR(10),
        PRIMARY KEY(downloaddate,uniqueadid,postdate));'''
        

tables = ['regionadcounts','regionjobadcounts','jobadpageurls','jobadpagedata']
    
#Report standard statistics and counts of the data that is in each of the tables    
def report_statistics():
    
    conn = sqlite3.connect("egyptOLX.db")
    c = conn.cursor()
    
    for i, t in enumerate(tables):
        query ='''SELECT * FROM {};'''
        temp = c.execute(query.format(t)).fetchall()
        print("Number of observations in table {}: {}".format(t,len(temp)))        
        query ='''PRAGMA table_info({});'''
        schema = c.execute(query.format(t)).fetchall()
        print(schema)
        
        if t == "jobadpageurls":
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
        
        
    query = '''SELECT DISTINCT region, freg, subregion, fsubreg, sector, urlregsector FROM regionjobadcounts;'''
    temp = c.execute(query).fetchall()
    print("Region-sectors: {}".format(len(temp)))
    
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

def update_table(tablename,newtablequery,insertstatement):
    
    conn = sqlite3.connect("egyptOLX.db")
    c = conn.cursor()

    query ='''PRAGMA table_info({});'''
    print(c.execute(query.format(tablename)).fetchall())    
    
    query = "ALTER TABLE {} RENAME TO {}_temp;"
    c.execute(query.format(tablename,tablename))
    
    c.execute(newtablequery)
    
    query = "INSERT INTO {} ({}) SELECT {} FROM {}_temp;"
    c.execute(query.format(tablename,insertstatement,insertstatement,tablename))
    
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

