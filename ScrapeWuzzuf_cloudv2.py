#####################################################################################################################
# # Scrape Wuzzuf Job Website
# 
# This code scrapes the Wuzzuf job website which has about ~5K jobs in Egypt for the assignment "Improving Labor Market 
# Matching in Egypt."
# 
# The Wuzzuf job website has detailed stats on the number of job applicants, reviewed applicants by employer, and number 
# of rejected applicants.  It also is all in English making it easier to synthesize the data.  The data is subsequently 
# output into a csv file that is further processed and cleaned.
# 
#    This code provides several improvements over the existing code and is desigend to be run once daily (at midnight Egypt time):
# 1. It deposits all scraped data into two SQL databases for easy querying.  These databases have consistency checks to make
# sure the entry is not a duplicate and is unique
# 2. It scrapes only the key pages that have been entered since the last download date
# 3. It re-scrape all pages that were posted 7, 14, 21, 28, 35, 42, 49 days ago.
# 4. It extracts all of the ad pages that are subsequently closed or no longer in existence into an archived database and subsequent CSV files  
# #### Created by Natalie Chun (December 2017)
########################################################################################################################

#amazon server runs on UTC time (8 hours ahead of PST)

#key packages for scraping websites
import urllib
import urllib.request as urlrequest
from bs4 import BeautifulSoup
import sys

#standard packages for holding and analyzing datasets and outputting files
import pandas as pd
import numpy as np
import re
import datetime
import time
import csv

start_time = time.time()

# open the sqlite and set the connection on the database
import sqlite3
conn = sqlite3.connect("wuzzuf.db")
c = conn.cursor()

#NOTE:  if want to reset or alter key tables in our database use 'WuzzufDatabaseConversion.py'

#display the information for the tables
query ='''PRAGMA table_info(urltable);'''
#print(c.execute(query).fetchall())
query ='''PRAGMA table_info(pagedata);'''
#print(c.execute(query).fetchall())
query ='''PRAGMA table_info(archivedpagedata);'''
#print(c.execute(query).fetchall())

# make sure current time/date refers to time in Egypt
from pytz import timezone
tz = timezone('Africa/Cairo')
datecur = datetime.datetime.now(tz)
print("Start Time: {}".format(datecur))
#print(datecur.strftime('%Y-%m-%d'))

# Run through all of the job ads and get the most recent job ad urls that have been posted to the website, 
# but have not yet been downloaded.  
# Have the URLs stored to a list which is returned
def get_WuzuffJobUrls(lastdownloaddate):

    url = 'https://wuzzuf.net/search/jobs?start=0&filters%5Bcountry%5D%5B0%5D=Egypt'
    nextpage = True
    urlinfo = []

    #check the dates of the pages that are listed
    while nextpage:
    
        req = urlrequest.Request(url)
        response = urlrequest.urlopen(req)
        soup = BeautifulSoup(response, 'html.parser')
    
        # objective is to get the links from the page and put it in a list to call and run through
        name_box = soup.find('div', attrs={'class': 'content-card card-has-jobs'})
        #print(name_box)

        #obtain all of the urls and dates associated with different jobs listed on the website (this only needs to be called once)
        for d in name_box.find_all('div', attrs={'class':'new-time'}):
            #print(d)
            temp= d.find('a',href=True)
            temptime =  d.find('time',title=True)
            dateval = datetime.datetime.strptime(temptime['title'],'%A, %B %d, %Y at %H:%M%p')
            url = temp['href'].split("?")[0]
            #print(url)
            temp = re.search(r'[jobs/p/|internship/](\d+)-',url)
            uniqueid = temp.group(1)
            urlinfo.append([uniqueid,dateval.strftime('%Y-%m-%d'),temptime['title'],url])
        
        # get the next set of job listings for this classification only if we have not already collected the data
        #print(dateval.date())
        if dateval.date() >= lastdownloaddate:
            nextpg = name_box.find('li', attrs={'class': 'pag-next'})
            try:
                url = nextpg.find_all('a', href=True)[0]['href']
                #Print out length to track number of urls retrieved
                #print(len(url_jobs))
                # sleep so that we do not bombard the website with requests
                time.sleep(3)
            except AttributeError:
                nextpage = False
        else:
            nextpage = False
    
    data = pd.DataFrame(data = urlinfo, columns = ['uniqueid','postdate','datetime','url'])

    return(data)

#Function keeps requesting page or until it hits limit of 5 requests
def request_until_succeed(url):
    req = urlrequest.Request(url)
    count = 1
    while count <= 5:
        try: 
            response = urlrequest.urlopen(req)
            if response.getcode() == 200:
                return(response)
        except Exception:
            time.sleep(1)
            print("Error for URL %s: %s" % (url, datetime.datetime.now()))
        count+=1
    return(None)
    
#this page scrapes individual job advertisement pages and returns the row of relevant data collected
punctuation = [";",",","'","&"]
def get_WuzzufJobData(uniqueid,urlname,postdate):

    ### note want to add in the actual time download if we are to use the page views as proxy       
    datetimecur = datetime.datetime.now(tz)
    downloaddate = datetimecur.strftime('%Y-%m-%d')
    downloadtime = datetimecur.strftime('%H:%M')

    #request the url page
    response = request_until_succeed(urlname)
    
    #if was not able to retrieve any of the data in 5 function calls then simply return the data empty
    if response is None:
         stat = 'NOT FOUND'
         job_data = [uniqueid, postdate, np.NAN, downloaddate, downloadtime, stat,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN]
         return(job_data)
    
    soup = BeautifulSoup(response, 'html.parser')
    #print(soup)
        
    #check job status and see if it is open or closed
    stat = "OPEN"
    status = soup.find('div',attrs={'class':"alert alert-danger alert-job col-sm-12"})
    
    if status is not None:
         stat = "CLOSED"
    
    #obtain main job data
    
    mainjobdata = soup.find('div', attrs={'class': 'job-main-card content-card'})
    
    #there is a few postings that may not be completely filled in
    if mainjobdata is None:
         job_data = [uniqueid, postdate, np.NAN, downloaddate, downloadtime, stat,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN,np.NAN]
         return(job_data)

    #print(mainjobdata)
    jobdata = mainjobdata.find_all(['h1','a','span'])
    #print(jobdata)
    jobinfo = {}

    for d in jobdata:
         try:
              if d['class'][0] in ['job-title','job-company-name','job-company-location']:
                   jobinfo[d['class'][0]] = d.get_text().strip().encode('utf-8')
                   #print(jobinfo[d['class'][0]])
         except KeyError:
              pass

    #get stats on applicants
    try:
        num_applicants = mainjobdata.find_all('div', attrs={'class': 'applicants-num'})[0].get_text()
    except IndexError:
        num_applicants = 0
        
    try:
        num_vacancies = mainjobdata.find_all('span', attrs={'class': 'vacancies-num'})[0].get_text()
    except IndexError:
        num_vacancies = 0

    stats = mainjobdata.find_all('div', attrs={'class': 'applicants-stat-num'})
    #print(stats)
    try:
        num_seen = stats[0].get_text()
    except IndexError:
        num_seen = 0
    try:
        num_shortlist = stats[1].get_text()
    except IndexError:
        num_shortlist = 0
    try:
        num_rejected = stats[2].get_text()
    except IndexError:
        num_rejected = 0
        
    #get date when posted and download date
    post_date = mainjobdata.find('p', attrs={'class': 'job-post-date'})
    #print(mainjobdata.find('time',datetime=True))
    #temp = mainjobdata.find('time',datetime=True)
    #print(temp['datetime'])
    #print(datetime.datetime.strptime(temp['datetime'],'%Y-%m-%dT%H:%M:%S'))
    
    #print(post_date['title'])
    try:
        pdate = datetime.datetime.strptime(post_date['title'],'%A, %B %d, %Y at %I:%M%p')
    except ValueError:
        pdate = datetime.datetime.strptime(post_date['title'],'%A, %B %d, %Y at%I:%M%p')
    postdate = pdate.strftime('%Y-%m-%d')
    posttime = pdate.strftime('%H:%M') # best time format for spreadsheet programs
    
    #now still need to split the post-date into a term that is valid
    #print(post_date['title'])
    
    #obtain job summary information
    jobsumm = soup.find('div', attrs={'class': 'row job-summary'})
    jobsummdata = jobsumm.find_all(['dl'])
    #print(jobsumm)
    #print(jobdata)
    for d in jobsummdata:
        try:
            temp = re.sub('\s+',' ',d.get_text()).strip().split(":")
            name = re.sub('\s',"_",temp[0].lower())
            if name in ['languages']:
                jobinfo[name] = temp[1].strip().split(',')
                jobinfo['languages'] = '>'.join(jobinfo['languages'])
            elif name in ['salary']:
                if 'Negotiable' in temp[1].strip().split(','):
                    jobinfo[name] = temp[1].strip().split(',')
                else:
                    newtemp = temp[1].strip().replace(',','')
                    jobinfo[name] = [newtemp]
                jobinfo['salary']='>'.join(jobinfo['salary'])
            else:
                jobinfo[name] = temp[1].strip()
        except KeyError:
            pass
        
    #these columns are not consistent across jobs so need to take this into account
    columns = ['experience_needed','career_level','job_type','salary','education_level','gender','travel_frequency','languages','vacancies']
    for c in columns:
        if c not in jobinfo:
            jobinfo[c] = "NA"
       
    jobcard = soup.find('div', attrs={'class': "about-job content-card"})
    #print(jobcard)
    data = jobcard.find_all('div', attrs={'class': "labels-wrapper"})
    #print(data)
    jobroles = []
    for d in data:
        for role in d.find_all(['a']):
            jobroles.append(role.get_text().strip())    
    jobinfo['roles'] = '>'.join(jobroles)
    #print(jobroles)
        
    #obtain job requirements, key words, and industry indicators
    jobreqs = soup.find('div', attrs={'class': "job-requirements content-card"})
    #print(jobreqs)
    if jobreqs is not None:
        data = jobreqs.find_all('meta', content=True)
        keywords = []
        try:
            temp = data[0]['content']
            for t in temp.split(', '):
                keywords.append(t)
            jobinfo['keywords'] = keywords
        except IndexError:
            jobinfo['keywords'] = []
    else:
        jobinfo['keywords'] = []
    jobinfo['keywords']='>'.join(jobinfo['keywords']).encode('utf-8')
    #print(jobinfo['keywords'])
    
    try:
        data = jobreqs.find_all('li')
        reqs = []
        for d in data:
            temp = d.get_text().lower().strip('.')
            for p in punctuation:
                temp = temp.replace(';','')
            reqs.append(temp)
        jobinfo['requirements'] = reqs
        #print(reqs)
    except:
        jobinfo['requirements'] = []
    jobinfo['requirements']='>'.join(jobinfo['requirements']).encode('utf-8')
    
    industries = soup.find('div', attrs={"class": "industries labels-wrapper"})
    inds = []
    
    try:
        indust = industries.find_all(['a'])
        for ind in indust:
            inds.append(ind.get_text().strip())
    except:
        pass
    jobinfo['industries'] = '>'.join(inds).encode('utf-8')

    #print(jobinfo)
    # now let us return the dictionary entries to write to a csv file.  
    #Note that we may need to split so we do not have problem with commas
    job_data = [uniqueid, postdate, posttime, downloaddate, downloadtime, stat,jobinfo['job-title'],jobinfo['job-company-name'],jobinfo['job-company-location'],num_applicants,num_vacancies,num_seen,
               num_shortlist,num_rejected,jobinfo['experience_needed'],jobinfo['career_level'],jobinfo['job_type'],jobinfo['salary'],
               jobinfo['education_level'],jobinfo['gender'],jobinfo['travel_frequency'],jobinfo['languages'],jobinfo['vacancies'],jobinfo['roles'],jobinfo['keywords'],jobinfo['requirements'],jobinfo['industries']]
    
    return(job_data)
    
#query the latest data in the table that will inform our scraping tool
conn = sqlite3.connect("wuzzuf.db")
c = conn.cursor()
query = "SELECT MAX(postdate) FROM urltable"
lastdate = c.execute(query).fetchall()[0][0]
print("Last Date Downloaded: {}".format(lastdate))
if lastdate is None:
    #if no data is in the database lets insert from 29 days ago
    newdate = datecur - datetime.timedelta(days=29)
    #print(datetime.date(newdate.year,newdate.month,newdate.day))
    urldata = get_WuzuffJobUrls(datetime.date(newdate.year,newdate.month,newdate.day))
else:
    #if there is data in the database lets only insert data posted after the last date downloaded
    temp = lastdate.split('-')
    urldata = get_WuzuffJobUrls(datetime.date(int(temp[0]),int(temp[1]),int(temp[2])))
    
print("Maximum new pages to enter into urltable: {}".format(len(urldata)))
    
#insert all of the new data retrieved from the website into the urltable
for i, row in urldata.iterrows():
    #only inserts into table where there is unique id and postdate (else ignore's the entry)
    query = '''INSERT OR IGNORE INTO urltable (uniqueid,postdate,urlpostdatetime,urls) VALUES (?,?,?,?);'''
    c.execute(query, row)
conn.commit()

#query master table and see how many items have been inserted so far
temp = c.execute('''SELECT * FROM urltable;''').fetchall()
#print(len(temp))

#query the master table in order to insert into our main table holding the page data
datenow = datecur.strftime('%Y-%m-%d')

# rotates through the data and grabs the urls for the ads that have been posted today and each week up to 2 months prior
# This dataset only contains urls where the status is open (not closed)
jobpageurlquerylist = []

for i in range(0,8):
    d = i*7
    temp = c.execute('''SELECT uniqueid, urls, postdate FROM urltable WHERE DATE(postdate) == DATE('{}','-{} days');'''.format(datecur.strftime('%Y-%m-%d'),d)).fetchall()
    jobpageurlquerylist = jobpageurlquerylist + temp 

#check how many queries we will make based on the numbers
#these are the urls for which we want to re-sample and obtain the daily data.
print("Number of pages to query: {}".format(len(jobpageurlquerylist)))
#print(jobpageurlquerylist)

#run through the url pages and retrieve the information we are then going to insert this information into our key database
for urlinfo in jobpageurlquerylist:
    query = '''INSERT OR IGNORE INTO pagedata (uniqueid,postdate,posttime,downloaddate,downloadtime,stat,jobtitle,
    company,location,num_applicants,num_vacancies,num_seen,num_shortlisted,num_rejected,experience_needed,career_level,
    job_type, salary,education_level,gender,travel_frequency,languages,vacancies,roles,keywords,requirements,industries)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
    rowvalues = get_WuzzufJobData(urlinfo[0],urlinfo[1],urlinfo[2])
    #print(rowvalues)
    c.execute(query, rowvalues)
    conn.commit()
    
conn.commit() 

#Create an archived database where we select information out of the main job database and place it
#into a stored archived database we need to do this because the free cloud system only has limited amount of storage.
query = '''INSERT OR IGNORE INTO archivedpagedata SELECT * FROM pagedata WHERE uniqueid in (SELECT DISTINCT uniqueid FROM pagedata WHERE stat == 'CLOSED' OR DATE(postdate) == DATE('{}','-{} days'));'''
temp1 = c.execute(query.format(datecur,57))
#print(len(temp1.fetchall()))

query = '''DELETE FROM urltable WHERE uniqueid in (SELECT DISTINCT uniqueid FROM archivedpagedata);'''
c.execute(query)
temp2 = c.execute(query)
#print(len(temp2.fetchall()))

query = '''DELETE FROM pagedata WHERE uniqueid in (SELECT DISTINCT uniqueid FROM archivedpagedata);'''
c.execute(query)
temp3 = c.execute(query)
#print(len(temp3.fetchall()))

# DOCUMENT CONTENT IN THE DATABASES

query = '''SELECT * FROM urltable;'''
temp4 = c.execute(query)
print("Entries in table urltable: {}".format(len(temp4.fetchall())))

query = '''SELECT * FROM pagedata;'''
temp5 = c.execute(query)
print("Entries in table pagedata: {}".format(len(temp5.fetchall())))

query = '''SELECT * FROM archivedpagedata;'''
temp6 = c.execute(query)
print("Entries in table archivedpagedata: {}".format(len(temp6.fetchall())))

conn.commit() 

#ideally want to extract data from both page data and archived page data to place in csv file
#however we want to occassionally clean up the archived page data file so that there is no data in it any longer
#lets do this on the first day of each month so we just store this data (which is unique)
if datecur.day == 1:
    query = '''SELECT * FROM archivedpagedata;'''
    data = c.execute(query)
    datenow = datecur.strftime("%m%d%Y")
    #print("Date now:  datenow")
    print("Number of archived page entries: {}".format(len(data.fetchall())))
    with open('archivedpagedata_'+datenow+'.csv','w') as file:
        w = csv.writer(file)
        w.writerow(['uniqueid','postdate','posttime','downloaddate','downloadtime','stat','jobtitle',
        'company','location','num_applicants','num_vacancies','num_seen','num_shortlisted','num_rejected','experience_needed','career_level','job_type','salary','education_level','gender','travel_frequency',
        'languages','vacancies','roles','keywords','requirements','industries'])
        w.writerows(data)
    file.close()
        
    #clear information from the archivedpagedata
    query = '''DELETE FROM archivedpagedata WHERE uniqueid in (SELECT uniqueid FROM archivedpagedata);'''
    c.execute(query)
conn.commit()
c.close()

print("Run Time: {}".format(time.time()-start_time))