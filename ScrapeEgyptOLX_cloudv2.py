###############################################################################################################################
# Scrape OLX Website

# This was created for the paper "Improving Labor Market Matching in Egypt".  It does an initial scrape of the OLX websites which are  seemingly one of the most comprehensive sites for online job postings that are available in Egypt.  This code scrapes the following OLX data:
#1. Aggregate counts by region of ad postings
#2. Aggregate counts by region of job ad postings
#3. Counts by region and sector of job ad postings
#4. Individual job advertisement data, containing a) desired level of education b) job data c) experience d) position type e) advertisement details.

# Note as much of the advertisement details are contained in Arabic google translate is used to automatically translate the advertisements.  Ads supposedly stay alive for 3 months or until a person takes it down (whichever is shorter).

#The data is subsequently output into a csv file that can be read in for analysis.
#NOTE:  This version is for the cloud (as it was hard to figure out how to install googletrans on the cloud)

#### Created by Natalie Chun (December 2017)
#################################################################################################################################

import urllib
import urllib.request as urlrequest
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import datetime
import time
import csv

#set to True if are able to googletranslate otherwise set to false
translation = False
if translation == True:
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

#NOTE:  if want to create, reset or alter key tables in our database use 'OLXDatabaseConversion.py'

#FUNCTION:  url request helper, set to only request an item 5 times
def request_until_succeed(url):
    req = urlrequest.Request(url)
    count = 1
    while count <= 5:
        try: 
            response = urlrequest.urlopen(req)
            if response.getcode() == 200:
                return(response)
        except Exception:
            print("Exception")
            time.sleep(1)
            print("Error for URL %s: %s" % (url, datetime.datetime.now()))
        count+=1
    return(None)


#FUNCTION: obtains aggregate counts by region of general ad postings and job ad postings
#objective should be to generate time series data that can be stored in an SQL database
def get_OLXregiondata():

    datetimecur = datetime.datetime.now(tz)
    downloaddate = datetimecur.strftime('%Y-%m-%d')
    downloadtime = datetimecur.strftime('%H:%M')

    url = 'https://olx.com.eg/en/sitemap/regions/'
    req = urlrequest.Request(url)
    response = urlrequest.urlopen(req)
    soup = BeautifulSoup(response, 'html.parser')
    #print(soup)
    
    name_box = soup.find('div', attrs={'class': 'content text'})
    regions = name_box.find_all('div', attrs={'class':'bgef pding5_10 marginbott10 margintop20 clr'})
    subregions = name_box.find_all('div',attrs={'class':"clr marginbott10"})
    #print(len(regions))
    #print(len(subregions))

    regionname = []
    totalposts = []
    subregname = []
    subposts = []
    fregname = []
    fsubregname = []
    data = []
    
    for i, subreg in enumerate(subregions):
    #print(regions[i].get_text().strip())
        region = regions[i].get_text().strip()
        temp = region.split(' (')
        regionname.append(temp[0])
        totalposts.append(temp[1].strip(')')) 
        fregname.append(re.sub('\s+(\+\s)?','-',regionname[i].lower()))    
        #print(subreg)
        text = subreg.find_all('li')
        #print(text)
        for t in text:
            temp = t.get_text().strip()
            temp = temp.replace('\n','').split('(')
            #print(temp)
            subregname = temp[0].strip()
            subposts = temp[1].strip(')')
            #print(subregname,subposts)
            fsubregname = re.sub('''[\s(\+\s)?|\'|\.\s]''','-',subregname.lower())
            fsubregname = re.sub('[-](-)?(-)?','-',fsubregname)
            row = [downloaddate,downloadtime,regionname[i],fregname[i],subregname,fsubregname,totalposts[i],subposts]
            query = '''INSERT OR IGNORE INTO regionadcounts (downloaddate,downloadtime,region,freg,subregion,fsubreg,totalregposts,subposts) VALUES (?,?,?,?,?,?,?,?);'''
            c.execute(query, row)
    
    #commit entries to the database
    conn.commit()

# FUNCTION:  loops through the key industries above and regions to investigate the counts of postings
# under each heading
# an easier way is to just loop through the general regions  
    
def write_OLXregionjobdata(rowstart=0):
    
    # NOTE SHOULD SELECT ONLY MOST RECENT DOWNLOAD DATE OF DATA
    query = '''SELECT DISTINCT region, freg, subregion, fsubreg FROM regionadcounts WHERE DATE(downloaddate) >= DATE('{}', '-2 days');'''
    data = pd.read_sql(query.format(datecur.strftime('%Y-%m-%d')),conn)
    
    #loop through 365 qism areas to get job data
    for i, reg in data[rowstart:].iterrows():
        
        datetimecur = datetime.datetime.now(tz)
        downloaddate = datetimecur.strftime('%Y-%m-%d')
        downloadtime = datetimecur.strftime('%H:%M')
        region = reg['region']
        freg = reg['freg']
        subregion = reg['subregion']
        fsubreg = reg['fsubreg']
        url = 'https://olx.com.eg/en/jobs-services/' + fsubreg + '/'
        subregsector, subreghref = get_OLXJobUrls(url)
        
        #now want to output this data into the SQL database
        for sector, numposts in subregsector.items():
            #print(subreghref[sector])
            rowvalues = [downloaddate,downloadtime,region,freg,subregion,fsubreg,sector,subreghref[sector],numposts]
            query = '''INSERT OR IGNORE INTO regionjobadcounts (downloaddate,downloadtime,region,freg,subregion,fsubreg,sector,urlregsector,totalposts) VALUES (?,?,?,?,?,?,?,?,?);'''
            c.execute(query,rowvalues)
         
        if i % 20 == 0:
            print(i,rowvalues)
        # sleep to make sure not too many requests are being made
        conn.commit()
        #time.sleep(1)            

 
#FUNCTION: obtains all of the sector variables and associated reference link that will be input into our database
def get_OLXJobUrls(url):
    
    sector = {}
    href = {}
    
    req = urlrequest.Request(url)
    try:
        response = urlrequest.urlopen(req)
    #certain regions have no job postings
    except:
        return([sector,href])
        
        
    soup = BeautifulSoup(response, 'html.parser')

    #get counts of number of jobs in different areas
    name_box = soup.find_all('div', attrs={'class': 'wrapper'})
    
    for name in name_box:
        #print(name)
        newnames = name.find_all('a', attrs={'class' : 'topLink tdnone '})
        if len(newnames) > 0:
            for i, n in enumerate(newnames):
                #print(n)
                #print(n['href'])
                #print(n.find(href=True))
                #href.append(n.find('a', href=True))
                sect = n.find('span', attrs='link').get_text().strip()
                cnt = n.find('span', attrs='counter nowrap').get_text().strip().replace(',','')
                #export a tuple rather than dictionary
                sector[sect] = cnt
                href[sect] = n['href']
    #print(sector)
    #print(href)
    return([sector,href])
    
#FUNCTION:  gets all the job page urls from subregion-sector listings
#lets automate this so that it only downloads more recent data using last downloaddate
def get_OLXJobPageUrls(region,freg,subregion,fsubreg,jobsector,url,lastdownloaddate):
    
    #query the database to find out the last date of download
    
    urllist = []
    req = urlrequest.Request(url)
    response = urlrequest.urlopen(req)
    soup = BeautifulSoup(response, 'html.parser')
    
    #now find out the total number of pages available
    try:
        nextpage = soup.find('div',attrs={'class':'pager rel clr'})
        temp = nextpage.find('input',attrs={'type':"submit"})['class']
        totalpages = re.search(r'(\d+)',str(temp[1])).group(1)
        #print(temp,totalpages)
    except:
        totalpages = 1
    #print(totalpages)
    
    #set the minimum addate to the current date
    datetimecur = datetime.datetime.now(tz)
    minaddate = datetimecur.date()
    cnt=1
    #check that the minumum ad date for a page is greater than the last downloaddate
    while(minaddate >= lastdownloaddate and cnt <= int(totalpages)):
    #for i in range(1,int(totalpages)+1):
        #print("Enter")
                
        newurl = url
        if cnt > 1:
            newurl = url + '?page='+str(cnt)
        #print(cnt, newurl)
        req = urlrequest.Request(newurl)
        response = urlrequest.urlopen(req)
        soup = BeautifulSoup(response, 'html.parser')
        
        #get the current time
        datetimecur = datetime.datetime.now(tz)
        datecur = datetimecur.strftime("%Y-%m-%d")
    
        adlinks = soup.find_all('div',attrs={'class':'ads__item__info'})
        adphotos = soup.find_all('div',attrs={'class':"ads__item__photos-holder"})
        
        #now loop through all of the relevant data and grab the ad information
        for i, val1 in enumerate(adphotos):
            #print(i)
            val2 = adlinks[i]
            temp1 = val1.find('img')
            temp2 = val1.find('span',attrs={'class':"ads__item__paidicon icon paid"})
            temp3 = val1.find('a',attrs={'data-statkey':'ad.observed.list'})
            #there are some really old ads that are no longer active and do not have ids so we just skip over this
            if temp3 is not None:
                temp3a = temp3['class'][2]
                #print(temp3a)
                temp3b = re.search(r'{id:(\d+)}',temp3a)
                uniqueadid = temp3b.group(1)
                #print(temp1['src'])
                temp4 = val2.find('a',attrs={'class':"ads__item__title"})
                #print(temp4['href'])
                urllinkshort = temp4['href'].split('/ad/')[1]
                temp5 = val2.find('p',attrs={'class':'ads__item__date'})
                tempdate = temp5.get_text().strip()
                yr = datetimecur.year
                mt = datetimecur.month
                day = datetimecur.day
                if 'Today' in tempdate:
                    postdate = datecur
                elif 'Yesterday' in tempdate:
                    tempdatetime = datetimecur - datetime.timedelta(days=1)
                    postdate = tempdatetime.strftime("%Y-%m-%d")
                else:
                    #print(tempdate)
                    months = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                    tempdate2 = re.search(r'(\d+)\s+(\w+)',tempdate) 
                    day = tempdate2.group(1)
                    mt = months[tempdate2.group(2)]
                    #ads stay alive for three months tops need to catch cases when cross over between one year to next
                    if datetimecur.month - mt >= 0:
                        yr = datetimecur.year
                    else:
                        yr = datetimecur.year - 1
                    postdate = str(yr)+'-'+str(mt)+'-'+str(day)
        
                i_photo = 0
                i_featured = 0
                if 'jobs-services-thumb.png' not in temp1['src']:
                    i_photo = 1
                if temp2 is not None:
                    i_featured = 1
                rowvalues = [region,freg,subregion,fsubreg,jobsector,postdate,uniqueadid,i_photo,i_featured,urllinkshort]
                #print(rowvalues)
                query = '''INSERT OR IGNORE INTO jobadpageurls (region, freg, subregion, fsubreg, jobsector, postdate, uniqueadid, i_photo, i_featured,
                urllinkshort) VALUES(?,?,?,?,?,?,?,?,?,?);'''
                c.execute(query,rowvalues)
                conn.commit()
    
        #now store the last date retrieved as the midaddate
        #print(yr,mt,day)
        try:
            minaddate = datetime.date(int(yr),int(mt),int(day))
        except:
            minaddate = datetimecur.date()
        cnt+=1
        time.sleep(1)
    conn.commit()

 
#FUNCTION:  gets the data from each job page need to alter to insert into database
#Already checked and cleaned
def get_OLXJobData(uniqueadid, postdate, url):
    
    fields = ['Experience Level','Employment Type','Education Level','Type','Compensation']
    fielddata = {}
    
    #print(url)
    response = request_until_succeed(url)
    
    soup = BeautifulSoup(response, 'html.parser')
    
    #get content for ad posting data and check if available as some are no longer available
    addata = soup.find('span',attrs={'class':'pdingleft10 brlefte5'})
    
    ### note want to add in the actual time download if we are to use the page views as proxy    
    datetimecur = datetime.datetime.now(tz)
    downloaddate = datetimecur.strftime('%Y-%m-%d')
    downloadtime = datetimecur.strftime('%H:%M')

    adstatus = 'OPEN'
    # return NULL values if we cannot find the page any longer
    if addata is None:
        adstatus = 'CLOSED'
        return(downloaddate, downloadtime, uniqueadid, postdate, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, adstatus)        
    
    #scrape the page if it does exist
    addata = addata.get_text().strip()
    #print(addata)
    m = re.search(r"at (\d+:\d+, \d+ \w+ \d+), Ad ID: (\d+)",addata)
    date = m.group(1)
    adid = m.group(2)
    dateval = datetime.datetime.strptime(date,'%H:%M, %d %B %Y')
    postdate = dateval.strftime('%Y-%m-%d') # best time format for spreadsheet programs
    posttime = dateval.strftime('%H:%M')
    #print(uniqueadid,adid,postdate)
    
    #get title of job advertisement
    temptitle = soup.find('div',attrs={'class':"clr offerheadinner pding15 pdingright20"})
    title = temptitle.find('h1').get_text()
    content = soup.find('div', attrs={'class':"clr", 'id':'textContent'}).get_text().strip()

    #translate the text as needed
    if translation == True:
        translator = googletrans.Translator()
        content = translator.translate(content).text.replace('\r',' ').replace('\n','>').encode('utf-8')
        title = translator.translate(title).text.encode('utf-8')
        texttype = 'EN'
    else:
        content = content.replace('\r',' ').replace('\n','>').encode('utf-8')
        title = title.encode('utf-8')
        texttype = 'AR'
    
    #get main content related to job
    name_box = soup.find_all('div', attrs={'class': "clr descriptioncontent marginbott20"})
    
    for name in name_box:
        #print(name)
        newnames = name.find_all('td', attrs={'class' : 'col'})
        #print(newnames)
        for name in newnames:
            cat = name.find('th').get_text().strip()
            catval = name.find('td').get_text().strip()
            fielddata[cat] = catval
            #print(cat)
            #print(catval)

    #note that not all categories are always included in a job advertisement so we have to make sure there are contingencies
    for f in fields:
        if f not in fielddata:
            fielddata[f] = np.NAN

    views = soup.find_all('div',attrs={'class':'pdingtop10'})
    #print(views)
    for v in views:
        if 'Views' in str(v):
            m = re.search(r"Views:<strong>(\d+)</strong>", str(v))
            pageviews = int(m.group(1))
            #print(num_views)
            
    #get content related to compensation/price
    comp = soup.find('div', attrs={'class': "pricelabel tcenter"})
    if comp is not None:
        compensation = comp.get_text().strip().replace(',','').strip(' EGP')
        #print(compensation)
    else:
        compensation = fielddata['Compensation']
    try:
        compensation = int(compensation)
    except:
        compensation = np.NAN
    #print(compensation)
    
    #get content related to identity of user/poster of ad
    user = soup.find('div', attrs={'class':'user-box'})
    if user is not None:
        userhref = user.find('a')['href']
        #print(userhref)
        #print(userhref.split('/user/')[1].strip('/'))
        username=user.find('p', attrs={'class':'user-box__info__name'}).get_text().strip().encode('utf-8')
        userjoindate=user.find('p', attrs={'class':'user-box__info__age'}).get_text().strip()
        m=re.search(r'On site since\s+(\w+)\s+(\d+)',userjoindate)
        userjoinmonth = m.group(1)
        userjoinyear = int(m.group(2))
    else:
        userhref = np.NAN
        username = np.NAN
        userjoindate = np.NAN
        userjoinmonth = np.NAN
        userjoinyear = np.NAN
    
    #email available?
    emailinfo = soup.find('div', attrs={'class':"contactbox innerbox br3 bgfff rel"})
    emailavail = 0
    if emailinfo is not None:
        if 'Email Seller' in emailinfo.get_text():
            emailavail = 1

    #phone available?
    phoneinfo = soup.find('div', attrs={'class':"contactbox-indent rel brkword"})
    phoneavail = 0
    if phoneinfo is not None:
        if 'Show phone' in phoneinfo.get_text().strip():
            phoneavail = 1
    
    rowvalues = [downloaddate, downloadtime, uniqueadid, postdate, posttime, pageviews, title, fielddata['Experience Level'], fielddata['Education Level'], fielddata['Type'], fielddata['Employment Type'], compensation, content,texttype, userhref, username, userjoinmonth, userjoinyear, emailavail, phoneavail, adstatus]
    #print(rowvalues)
    #OLX field for compensation just got changed (17-Dec-2017)
    return(rowvalues)

#Test for OLXJobData Query
#get_OLXJobData(np.NAN, np.NAN, 'https://olx.com.eg/en/ad/-ID8t16m.html')

                
#FUNCTION:  create file listing of all job page urls (should be about ~100K)
#Note:  Need to alter this to potentially grab only the most recent data
#Note:  Need to grab indicator if ad is a featured ad
#Note:  Need to look for indicator 'Today' plus time
#Note:  Need to look at indicator 'Yesterday' plus time
#Note:  Then look for date which does not list the year.....will have to take into account when year switches over from 2017 to 2018.
#Note:  "ads__item__date"
#Note:  "ads has photo"  if not "ads__item__photos" https://olxegstatic-a.akamaihd.net/dc88180-1111/naspersclassifieds-regional/olxmena-atlas-web/static/img/categories-thumbs/jobs-services-thumb.png
query = '''INSERT OR IGNORE INTO jobadpageurls (region, freg, subregion, fsubreg, jobsector, postdate, posttime, uniqueadid, i_photo, i_featured,
        urllinkshort VALUES(?,?,?,?,?,?,?,?,?,?,?);'''

def write_OLXJobUrls(lastdownloaddate, sector,datast=0):
    #sector names
    sectornames = []
    for key, val in sector.items():
        if key != 'Jobs Wanted':
            sectornames.append(key)
        
    #loop through 365 qism areas to get job data
    for i, reg in data[datast:].iterrows():
            
        print(reg['fsubregname'])
        #write each regional data to a separate file to reduce having to re-do downloads in case of failure
        with open('OLX_joburls_'+reg['fsubregname']+'_'+dateval+'.csv', 'w', newline='') as file:
            w = csv.writer(file)
            w.writerow(["sector","region","subregion","jobpageurl"])
            
            fsubregname = reg['fsubregname']
            url = 'https://olx.com.eg/en/jobs-services/' + fsubregname + '/'
            subregsector, subreghref = get_OLXJobUrls(url)
            
            #now want to grab each of the urls for each subreg and subsector
            for subregsector, href in subreghref.items():
                #print(href)
                urlpages = get_OLXJobPageUrls(freg,fsubreg,subregsector,href,lastdownloaddate)
                #print(len(urlpages))
                for urlp in urlpages:
                    w.writerow([subregsector,reg['region'],reg['subregion'],urlp])
            if i % 20 == 0:
                print(subregsector,reg['region'],reg['subregion'],urlp)
        file.close()
                
#FUNCTION:  write queries from each job page data into the SQL database
#NOTE:  STILL NEED TO FIX THIS
def write_OLXjobpagedata(datast=0):
    
    #sector names
    sectornames = []
    for key, val in sector.items():
        sectornames.append(key)
    
    urllist = {}
    
    op = 'a'
    if datast == 0:
        op = 'w'
    
    #write out the data for each job page
    with open('OLX_jobpagedata_'+dateval+'.csv', op, newline='') as file:
        w = csv.writer(file)
        if datast == 0:
            w.writerow(["downloaddate","id","url","postdate","sector","region","subregion","jobcontent","num_views","explevel","emptype","educlevel","employertype","compensation"])
    
        #loop through 365 qism areas to call the files containing the different urls
        for i, reg in data[datast:].iterrows():
            fsubregname = reg['fsubregname']
            print(fsubregname)
            urllist[fsubregname]=pd.read_csv('OLX_joburls_'+reg['fsubregname']+'_'+dateval+'.csv')
            for j, row in urllist[fsubregname].iterrows():
                temp = row['jobpageurl'].split('ad/')[1]
                newurl = 'https://olx.com.eg/en/ad/'+temp
                rowdata = get_OLXjobdata(newurl,row['sector'],row['region'],row['subregion'])
                w.writerow(rowdata)
                time.sleep(1)   
  
    file.close()
                
                
#write one entry per day to the OLXregiondata job database
get_OLXregiondata()

#Loop through each of the subregions and get the job data counts by sector
#NOTE:  Because this program runs so slow we should check whether entries exist or not.  If exist do not re-survey.....can start the row sampling from 0 to 365 (good for testing)

write_OLXregionjobdata(rowstart=0)
print("Run Time to execute write_OLXregionjobdata: {}".format(time.time()-start_time))

#Now insert new data into table jobadpageurls (we probably should query on both sector and subregion since the website is very slow)
#loop through ~2697 region-qism areas to get job data (this is quite substantial) how to do less?
datast = 0
#select only region sectors where total posts have changed at least once over the last 5 days
query = '''SELECT DISTINCT a.region, a.freg, a.subregion, a.fsubreg, a.sector, a.urlregsector FROM regionjobadcounts a INNER JOIN regionjobadcounts b ON a.region = b.region AND a.subregion = b.subregion AND a.sector = b.sector WHERE DATE(a.downloaddate,'+5 DAYS') >= (SELECT DISTINCT MAX(DATE(downloaddate)) FROM regionjobadcounts) AND (a.totalposts != b.totalposts) AND DATE(a.downloaddate,'-2 DAYS') == DATE(b.downloaddate);'''

regsector = c.execute(query).fetchall()
print("Region-sectors to grab: {}".format(len(regsector)))


# NOTE:  A more efficient query will only hit the pages where there is likely to have been a change in 
# job ads entered from the last time we have accessed the dataset.... (STILL NEED TO PROGRAM THIS)

for i, reg in enumerate(regsector[datast:]):
    query = '''SELECT uniqueadid FROM jobadpageurls WHERE fsubreg = '{}' AND jobsector = '{}';'''
    ids = c.execute(query.format(reg[3],reg[4])).fetchall()
    #print(ids)
    query = '''SELECT MAX(DATE(downloaddate)) FROM jobadpagedata WHERE uniqueadid IN (SELECT uniqueadid FROM jobadpageurls WHERE fsubreg = '{}' AND jobsector = '{}');'''
    lastdate = c.execute(query.format(reg[3],reg[4])).fetchall()[0][0]
    print("Last Download Date for sub-region {} and sector {}: {}".format(reg[3],reg[4],lastdate))
    query = '''SELECT COUNT(*) FROM jobadpageurls WHERE fsubreg == '{}' AND jobsector == '{}';'''
    oldnumentries = c.execute(query.format(reg[3],reg[4])).fetchall()[0][0]
    #print("Old numentries: {}".format(oldnumentries))
    if lastdate is None:
        #if no data is in the database lets insert from X days ago
        newdate = datecur - datetime.timedelta(days=30)
        #print(datetime.date(newdate.year,newdate.month,newdate.day))
        #print("I : {}".format(i))
        get_OLXJobPageUrls(reg[0],reg[1],reg[2],reg[3],reg[4],reg[5],datetime.date(newdate.year,newdate.month,newdate.day))
    else:
        date = lastdate.split(' ')[0]
        temp = date.split('-')
        #if there is data in the database lets only insert data posted after the last date downloaded
        get_OLXJobPageUrls(reg[0],reg[1],reg[2],reg[3],reg[4],reg[5],datetime.date(int(temp[0]),int(temp[1]),int(temp[2])))
    query = '''SELECT COUNT(*) FROM jobadpageurls WHERE fsubreg == '{}' AND jobsector == '{}';'''
    newnumentries = c.execute(query.format(reg[3],reg[4])).fetchall()[0][0]
    print("Number new pages to entered into jobadpageurls for subregion {} and sector {}: {}".format(reg[3],reg[4],newnumentries-oldnumentries))
    
#now that we have all of the relevant new urls we want to query on the new urls where we want to grab the data and insert it into the database
#note the question is how important is it that we obtain data over time for the job AD URLS or simply track distinct listings?

# rotates through the data and grabs the urls for the ads that have been posted today and each week up to 2 months prior
# This dataset only contains urls where the status is open (not closed)

jobpageurllist = []

#Ads are up for a maximum of 3 months so if we rotate through for at least 15 weeks this should cover everything
#NOTE:  TO REDUCE THE RISK THAT WE QUERY TOO MANY FILES TRY TO DROP ALL URLS THAT NO LONGER EXIST FROM jobadpageurls
for i in range(0,15):
    d = i*7
    temp = c.execute('''SELECT uniqueadid, postdate, urllinkshort FROM jobadpageurls WHERE DATE(postdate) == DATE('{}','-{} days') and uniqueadid NOT IN (SELECT DISTINCT uniqueadid FROM jobadpagedata WHERE adstatus == 'CLOSED' OR DATE(postdate) <= DATE('{}','-93 days'));'''.format(datecur.strftime('%Y-%m-%d'),d,datecur)).fetchall()
    jobpageurllist = jobpageurllist + temp

#check how many queries we will make based on the numbers
#these are the urls for which we want to re-sample and obtain the daily data.
print("Number of pages to query: {}".format(len(jobpageurllist)))
#print(jobpageurlquerylist)

#run through the url pages and retrieve the information we are then going to insert this information into our key database
    
for urlinfo in jobpageurllist:
    query = '''INSERT OR IGNORE INTO jobadpagedata (downloaddate,downloadtime,uniqueadid,postdate,posttime,pageviews,title,experiencelevel,educationlevel,type,employtype,compensation,description,textlanguage,userhref,username,userjoinmt,userjoinyear,emailavail,phoneavail,
    adstatus)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
    url = 'https://olx.com.eg/en/ad/'+urlinfo[2]
    rowvalues = get_OLXJobData(urlinfo[0],urlinfo[1],url)
    #print(rowvalues)
    c.execute(query, rowvalues)
conn.commit() 

#NEED TO THINK ABOUT HOW TO ARCHIVE A SUBSET OF THE DATA ON OCCASSION......(FOR FUTURE)

# DOCUMENT CONTENT IN THE DATABASES

query = '''SELECT * FROM regionadcounts;'''
temp = c.execute(query)
print("Entries in table regionadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT DISTINCT downloaddate FROM regionadcounts;'''
temp = c.execute(query)
print("Distinct entry dates in table regionadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT * FROM regionjobadcounts;'''
temp = c.execute(query)
print("Entries in table regionjobadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT DISTINCT sector FROM regionjobadcounts;'''
temp = c.execute(query)
print("Distinct sector in table regionjobadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT DISTINCT fsubreg FROM regionjobadcounts;'''
temp = c.execute(query)
print("Distinct subregions in table regionjobadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT DISTINCT downloaddate FROM regionjobadcounts;'''
temp = c.execute(query)
print("Distinct entry dates in table regionjobadcounts: {}".format(len(temp.fetchall())))

query = '''SELECT downloaddate, uniqueadid FROM jobadpagedata;'''
temp = c.execute(query)
print("Number of ad entries in table jobadpagedata: {}".format(len(temp.fetchall())))

query = '''SELECT DISTINCT uniqueadid FROM jobadpagedata;'''
temp = c.execute(query)
print("Distinct ads in table jobadpagedata: {}".format(len(temp.fetchall())))

#Produce some summary statistics that convey the quality of the job scrape


conn.commit() 
conn.close()


print("Run Time: {}".format(time.time()-start_time))