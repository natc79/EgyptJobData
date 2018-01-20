##################################################################################################################
# Analyze OLX Data Scraped from Website
# This was created for the paper "Improving Labor Market Matching in Egypt".

#The data is subsequently output into a csv file and processed via google translate
#NOTE:  This version is linked to the the data scraped in the cloud (as it was hard to figure out how to install googletrans on the cloud)

#### Created by Natalie Chun (December 2017)
#################################################################################################################

import pandas as pd
import numpy as np
import re
import datetime
import time
import csv
import googletrans

#time on the website is listed in Egypt time
from pytz import timezone
tz = timezone('Africa/Cairo')
datecur = datetime.datetime.now(tz)
datenow = datecur.strftime("%m%d%Y")

print("Start Time: {}".format(datecur))
start_time = time.time()

# open the sqlite and set the connection on the database
import sqlite3
conn = sqlite3.connect("egyptOLX.db")
c = conn.cursor()

#import the key dataset

def combine_data():

    #first combine the relevant datasets
    query = '''SELECT a.downloaddate, a.downloadtime, b.region, b.subregion, b.jobsector, a.postdate, a.posttime, a.uniqueadid, b.i_photo, b.i_featured, a.pageviews, a.title, a.experiencelevel, a.educationlevel, a.type, a.employtype, a.compensation, a.description, a.textlanguage, a.userhref, a.username, a.userjoinyear, a.userjoinmt, a.emailavail, a.phoneavail, a.adstatus FROM jobadpagedata a LEFT JOIN jobadpageurls b ON a.uniqueadid == b.uniqueadid AND a.postdate == b.postdate;'''
    unprocdata = pd.read_sql(query,conn,parse_dates=['postdate','downloaddate'])

    #now get any data that might exists in the various csv files that have been archived
    archivedfiles = []
    if len(archivedfiles) > 0:
        for date in archivedfiles:
            filename = 'archivedpagedata_'+date+'.csv'
            tempdata = pd.read_csv(filename,parse_dates=['postdate','downloaddate'])
            #NOTE:  append tempdata to the unprocdata file maybe have to check that the data is in same format
            unprocdata = unprocdata.append(tempdata,ignore_index=True)
        
    c.close()
    print("Number of distinct entries: {}".format(len(unprocdata)))
    print(unprocdata.dtypes)
    print(unprocdata.head())
    print(unprocdata['postdate'].value_counts())
    print(unprocdata['educationlevel'].value_counts())
    print(unprocdata['experiencelevel'].value_counts())
    print(unprocdata['type'].value_counts())
    print(unprocdata['employtype'].value_counts())
    print(unprocdata['userjoinyear'].value_counts())
    print(unprocdata['adstatus'].value_counts())
    print(unprocdata['i_photo'].value_counts())
    print(unprocdata['i_featured'].value_counts())
    print("Memory Usage (mb): {}".format(round(unprocdata.memory_usage(deep=True).sum()/1048576,2)))
    return(unprocdata)
     
unprocdata = combine_data() 

def clean_data(data):
    translator = googletrans.Translator()
    print(data['description'].value_counts())
    data['description_english'] = [translator.translate(desc).text for desc in data['description'].decode('utf-8')]
    print(data['description_english'].head(50))
    data['bachelor_degree'] = [1 if row in ['Bachelors Degree','Masters Degree','PhD'] else 0 for row in data['educationlevel']]
    data['fulltime'] = [1 if row in ['Full-time'] else 0 for row in data['type']]
    data['comp'] = [float(str(row).replace(',','')) if row != np.nan else np.nan for row in data['compensation']]
    data['exp_management'] = [1 if row in ['Management','Executive/Director','Senior Executive (President, CEO)'] else 0 for row in data['experiencelevel']]
    data['exp_entrylevel'] = [1 if row in ['Entry level'] else 0 for row in data['experiencelevel']]
    keeprows = (data['jobsector'] != 'Jobs Wanted') & (data['employtype'] != 'Job Seeker')
    print(data['fulltime'].value_counts())
    print(keeprows[0:10])
    jobvacancies = data[keeprows == True]
    summarystats = jobvacancies.groupby('jobsector').mean()
    tempstats = jobvacancies.groupby('jobsector')['uniqueadid'].count()
    print(tempstats)
    print(summarystats)
    summarystats.to_csv('summary_statistics_OLX.csv')
    
clean_data(unprocdata)

conn.close()

