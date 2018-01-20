##########################################################################################################

# Analyze Wuzzuf Data
# This data draws on data scraped from Wuzzuf.com related to job advertisement in Egypt to:

# 1. cleans the data so it is useful for regression analysis 
# 2. select out key nouns and words from the requirements section of the job advertisements
# 3. produce word clouds that highlight key skills and qualifications desired for different occupations

# Code was developed for paper "Improving Labor Market Matching in Egypt"
# Code was updated to extract information from the SQl database
# And the archivedpagedata that is stored in various csv files

#### Created by Natalie Chun (20 September - 15 October 2017)
##########################################################################################################

# import relevant packages for processing data

import csv
import pandas as pd
import numpy as np
import re
from datetime import datetime
import nltk
import time
import re

import sqlite3
conn = sqlite3.connect("wuzzuf.db")
c = conn.cursor()

start_time = time.time()

#STEP 1:  Combine all of the unprocessed data into a single pandas dataframe

def combine_data():

    #extract the relevant set of data
    query = '''SELECT * FROM pagedata;'''
    unprocdata = pd.read_sql(query,conn,parse_dates=['postdate','downloaddate'])
    query = '''SELECT * FROM archivedpagedata;'''
    unprocarchiveddata = pd.read_sql(query,conn, parse_dates=['postdate','downloaddate'])
    unprocdata = unprocdata.append(unprocarchiveddata,ignore_index=True)

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
    print("Memory Usage (mb): {}".format(round(unprocdata.memory_usage(deep=True).sum()/1048576,2)))
    return(unprocdata)
        
unprocdata = combine_data()
        
#STEP 2:  Start working with the unprocessed data to process it
        
#read the following items into dictionary that will be used for mapping industry and job data
with open('wuzzuf_industry_list_mapping.csv', mode='r') as infile:
    reader = csv.reader(infile)
    ind_mapping = {rows[0]:rows[1] for rows in reader}
    infile.close()

with open('wuzzuf_job_list_mapping.csv', mode='r') as infile:
    reader = csv.reader(infile)
    job_mapping = {rows[0]:rows[1] for rows in reader}
    infile.close()

#this checks the text in the requirements section to identify key words to focus on
#it cleans the requirements section data that will be better for processing and analysis
def clean_requirements(dataset):
    keywords = []
    alltext = []
    notation = ['\"','''\'''',"-","(",")","[","]",";","."]
    for i, row in dataset.iterrows():
        # Replace any non-letter, space, or digit character in the headlines.
        linetext = []
        #note that a lot of the data is stored with '>' to denote different lines 
        text = row['requirements'].decode('utf-8').strip("[").strip("]").split('>')
        #print(text)
        for t in text:
            #print(t)
            t = t.strip()
            t = re.sub(r'^b[\'|\"]','',t)
            t = re.sub(r'\\x[\\x|\w+]','',t)
            t = re.sub(r'[\d]',"",t)
            t = re.sub('\s+'," ",t)
            for n in notation:
                t = t.replace(n,"")
            t = re.sub(r'(bs/ba|university|bachelors|masters)','bachelor',t)
            t = re.sub(r'leaders[\s|$]','leadership',t)
            t = re.sub(r'[^|\s]ms\s','microsoft',t)
            t = re.sub(r'good looking','good-looking',t)
            t = re.sub(r'hard working','hard-working',t)
            t = re.sub(r'attention to detail','attention-to-detail',t)
            t = t.replace('\\','')
            linetext.append(t)
        alltext.append(' '.join(linetext))
    return(alltext)

    
# cleans the requirements text by removing stop words and focusing on nouns
def clean_text(text, MOSTCOMMON = True):
    
    #use the built in list of stop words 
    from nltk.corpus import stopwords
    stop = set(stopwords.words('english'))
    
    #place things into parts of speech and label as noun, verb etc.
    tokenized = nltk.tokenize.word_tokenize(text)
    pos_words = set(nltk.pos_tag(tokenized))
    #print(tokenized)
    #print(text.split())
    
    # eliminate
    morestop = ['experience','user','skills','skill','ability','excellent','years','year','relevant',
                'knowledge','work','minimum','time','command', 'field','degree','familiar','must','strong','good',
               'preferred','plus','able','least','must','using','understanding','background','presentable','candidate',
               'able','ability','least','working','including','related','required','solid','attention']

    clean_split = [w for w in text.split() if w not in stop and w not in morestop and len(w)>=4]
    merged_clean = ' '.join(clean_split)
    
    NOUNS = ['NN', 'NNS', 'NNP', 'NNPS']
    cnt_words = nltk.FreqDist(clean_split)
    
    bigrams = list(nltk.bigrams(clean_split))
    cnt_bigrams = nltk.FreqDist(bigrams)
    
    if MOSTCOMMON is True:
        most_freq_nouns = [w for w, cnt in cnt_words.most_common(30) if nltk.pos_tag([w])[0][1] in NOUNS]
        #print(bigrams)
        most_freq_bigrams = [w for w, cnt in cnt_bigrams.most_common(30) if w[0] in most_freq_nouns or w[1] in most_freq_nouns and w[0] != w[1]]
        #print(cnt_bigrams.most_common(100))
        return(merged_clean,most_freq_nouns,most_freq_bigrams)
    else:
        
        keyunigrams = ['emails','analytics','analytical','research','bachelor','leadership',
                       'interpersonal','communication','knowledge','marketing','management','business','computer','team','english','arabic',
                       'presenting','confident','travel','initiative',
                       'arabic','writing','reading','negotiation','excel','powerpoint', 'flexible','microsoft','persuade','strategic']
        keybigrams = [('meeting', 'clients'),('microsoft','office'),('organizational','ability'),
                      ('time','management'),('problem','solving'),('data','driven'),
                      ('sales','oriented')]
        other_languages = ['Chinese','Turkish','French','German','Russian','Spanish']
    

#extracts key skills
def tag_skills(textline,word_tags,bigram_tags):
        
    cnt_tags = 0
    cleanline, words, bigrams = clean_text(textline)
    for word in words:
        #print(word)
        if word in word_tags:
            cnt_tags += 1
    for bigram in bigrams:
        if bigram in bigram_tags:
            cnt_tags += 1
    return(cnt_tags)
    
#function cleans the different job page data information
def clean_jobdata(jobdata):
    
    # create new variables from the jobdata
    jobdata['no_company_identity'] = [1 if 'Confidential Company' in row['company'].decode('utf-8') else 0 for i, row in jobdata.iterrows()]
    jobdata['fulltime'] = [1 if 'Full Time' in row['job_type'] else 0 for i, row in jobdata.iterrows()]
    jobdata['parttime'] = [1 if 'Part Time' in row['job_type'] else 0 for i, row in jobdata.iterrows()]
    jobdata['contract'] = [1 if 'Contract' in row['job_type'] else 0 for i, row in jobdata.iterrows()]
    jobdata['min_bachelors'] = [1 if row['education_level'] is not np.NAN and 'Some Schooling at least' not in row['education_level'] else 0 for i, row in jobdata.iterrows()]
    jobdata['gender_female'] = [1 if row['gender'] is not np.NAN and 'Females' in row['gender'] else 0 for i, row in jobdata.iterrows()]
    jobdata['gender_male'] = [1 if row['gender'] is not np.NAN and 'Males' in row['gender'] else 0 for i, row in jobdata.iterrows()]
    jobdata['some_travel'] = [1 if row['travel_frequency'] is not np.NAN else 0 for i, row in jobdata.iterrows()]
    jobdata['language_English'] = [1 if row['languages'] is not np.NAN and 'English' in row['languages'] else 0 for i, row in jobdata.iterrows()]
    jobdata['language_Arabic'] = [1 if row['languages'] is not np.NAN and 'Arabic' in row['languages'] else 0 for i, row  in jobdata.iterrows()]
    jobdata['requirements_num'] = [len(row['requirements'].decode('utf-8').split('>')) for i, row in jobdata.iterrows()]    
    jobdata['requirement_bachelor'] = [1 if 'bachelor' in row['requirements'].decode('utf-8') else 0 for i, row in jobdata.iterrows()]
    
    #analyze requirements and count
    tags_nouns_hard = ['experience','knowledge','degree','computer','software','engineering','science','language','excel']
    tags_nouns_soft = ['communication','team','presentation','negotiation','leadership','interpersonal']
    tags_bigrams_soft = [('problem', 'solving'),('work', 'pressure'),('time', 'management'),('attention', 'detail')]
    tags_bigrams_hard = [('microsoft','office')]  
    jobdata['soft_skills'] = [tag_skills(row['clean_requirements'],tags_nouns_soft,tags_bigrams_soft) for i, row in jobdata.iterrows()]
    jobdata['hard_skills'] = [tag_skills(row['clean_requirements'],tags_nouns_hard,tags_bigrams_hard) for i, row in jobdata.iterrows()]
    
    experience_min = []
    experience_max = []
    vacancies = []
    p1 = re.compile('^(\d) to (\d) years')
    p2 = re.compile('^More than (\d) years')
    p3 = re.compile('^Less than (\d) years')
    p4 = re.compile('^(\d) years')
    p5 = re.compile('^(\d) open position')
    for i, row in jobdata.iterrows():
        values1 = p1.search(row['experience_needed'])
        values2 = p2.search(row['experience_needed'])
        values3 = p3.search(row['experience_needed'])
        values4 = p4.search(row['experience_needed'])

        if values1 is not None:
            experience_min.append(int(values1.group(1)))
            experience_max.append(int(values1.group(2)))
        elif values2 is not None:
            experience_min.append(int(values2.group(1)))
            experience_max.append(30)
        elif values3 is not None:
            experience_min.append(0)
            experience_max.append(int(values3.group(1)))
        elif values4 is not None:
            experience_min.append(int(values4.group(1)))
            experience_max.append(int(values4.group(1)))
        else:
            experience_min.append(0)
            experience_max.append(30)
            
        values5 = p5.search(row['vacancies'])
        if values5 is not None:
            vacancies.append(values5.group(1))
        else:
            vacancies.append(1)
            
    industry_cats = {}
    job_cats = {}
    clean_items = ['[',']','''\'''']
    datevalues = []
    location = []
    location_list = []
    indlist = {}
    joblist = {}
    
    for i in range(0,3):
        indlist[i]=[]
        joblist[i]=[]
        
    for i, row in jobdata.iterrows():
        
        #clean industry and job values
        industries = row['industries'].decode('utf-8').split(">")
        job_roles = row['roles'].split(">")
        for i in range(0,3):
            try:
                for c in clean_items:
                    industries[i]=industries[i].strip().strip(c)
                indlist[i].append(industries[i])
            except:
                indlist[i].append(np.NAN)
            
            try:
                for c in clean_items:
                    job_roles[i]=job_roles[i].strip().strip(c)
                joblist[i].append(job_roles[i])
            except:
                joblist[i].append(np.NAN)
                
        address = row['location'].decode('utf-8').strip("\'")
        if 'Egypt' not in address:
            address = address + ", Egypt"
        location.append(address)                
        
    jobdata['address'] = location
    for i, v in enumerate(['jobrole0','jobrole1','jobrole2']):
        jobdata[v] = joblist[i]
        jobdata[v] = jobdata[v].replace(job_mapping)
    for i, v in enumerate(['indtype0','indtype1','indtype2']):
        jobdata[v] = indlist[i]
        jobdata[v] = jobdata[v].replace(ind_mapping)
    jobdata['experience_min'] = experience_min
    jobdata['experience_max'] = experience_max
    jobdata['days_posted'] = jobdata['downloaddate']-jobdata['postdate']
    
    #create industry and job dummies based on content in multiple columns
    indkeys = set(jobdata['indtype0'])
    jobkeys = set(jobdata['jobrole0'])
    
    for i, ind in enumerate(indkeys):
        jobdata['ind'+str(i)] = [1 if ind in [row['indtype0'],row['indtype1'],row['indtype2']] else 0 for j, row in jobdata.iterrows()]
    for i, job in enumerate(jobkeys):
        jobdata['job'+str(i)] = [1 if job in [row['jobrole0'],row['jobrole1'],row['jobrole2']] else 0 for j, row in jobdata.iterrows()]
    
    #indicators for managerial role
    jobdata['career_manager'] = [1 if row['career_level'] == "Manager" or row['career_level'] == 'Senior Management (e.g. VP, CEO)' else 0 for i, row in jobdata.iterrows()]
    
    #obtain location counts of our data
    newlocation = []
    location = ['Alexandria','Assuit','Cairo','Gharbia','Giza','Ismailia','Kafr Alsheikh','Matruh','Monufya','Port Said','Sharqia','Qalubia']
    for i, row in jobdata.iterrows():
        address = "Other"
        for l in location:
            if l in row['address']:
                address = l
        newlocation.append(address)
    
    jobdata['newlocation'] = newlocation
    
    #create indicators for the location
    jobdata['Cairo'] = [1 if row['newlocation'] == 'Cairo' else 0 for i, row in jobdata.iterrows()]
    jobdata['Giza'] = [1 if row['newlocation'] == 'Giza' else 0 for i, row in jobdata.iterrows()]
    jobdata['Alexandria'] = [1 if row['newlocation'] == 'Alexandria' else 0 for i, row in jobdata.iterrows()]
    jobdata['Other_region'] = [1 if row['newlocation'] not in ['Alexandria','Cairo','Giza'] else 0 for i, row in jobdata.iterrows()]
    jobdata['career_entry'] = [1 if row['career_level'] == "Entry Level" or row['career_level'] == 'Student' else 0 for i, row in jobdata.iterrows()]
    
    #export the processed data to a csv file
    from pytz import timezone
    tz = timezone('Africa/Cairo')
    datecur = datetime.now(tz)
    datenow = datecur.strftime("%m%d%Y")
    jobdata.to_csv('wuzzuf_jobdata_processed_'+datenow+'.csv')
    
    return(jobdata)
    
unprocdata['clean_requirements'] = clean_requirements(unprocdata)
cleandata = clean_jobdata(unprocdata)
print(cleandata.head())

#obtain location counts of our data
print(cleandata['newlocation'].value_counts())
print(cleandata['jobrole0'].value_counts())
print(cleandata['jobrole1'].value_counts())

#STEP 3:  Print Out Key Summary Statistics of Our New Data
countobs = cleandata.groupby('downloaddate').count()
summarystats = cleandata.groupby('downloaddate').aggregate([np.mean, np.std])
print(countobs)
print(summarystats)
countobs.to_csv("Wuzzuf_variablecounts.csv")
summarystats.to_csv('Wuzzuf_summarystats.csv')

#STEP 4:  START TAGGING KEY WORDS ASSOCIATED WITH DIFFERENT JOB TYPES
#This can be important for creating word clouds, but also for coming up with measures on the relative complexity of jobs


		
		
print("Run Time: {}".format(time.time()-start_time))