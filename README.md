# EgyptJobData

This repository scrapes several job websites in Egypt to gather information on labor market demands.  This information can be useful for tracking occupational trends over time, informing educational investments and the design of curriculums that can help improve labor market outcomes.  The two repositories that are scraped are:

1. OLX.com
2. Wuzzuf.net

OLX.com contains nearly 130K job ads at any given time with around one-third of these ads applying to job seekers and the other two-thirds of job ads applying to open job vacancies.  Most of these job ads are listed in Arabic and therefore capture a good deal of the local market.  The geographic coverage of these ads is extensive covering nearly 365 regions and all 27 governates of Egypt. Each job ad goes live for approximately 90 days or until removal by the user.  The extent to which these ads are potentially valid and truely representative of real job vacancies requires further investigation.  Frequency and extent to job postings may also be somewhat correlated with the availability and accessibility of the internet in different areas nevertheless it was assessed as one of the better sources for gathering job information online in the context of Egypt.

Wuzzuf.net is a job platform that is targeted at export oriented jobs and jobs in foreign companies.  It contains about 5K job ads at any given time with job ads typically expiring after 30 days.  Most job ads are listed in English.  The Wuzzuf is a professionalized platform where employers with job vacancies fill in many details about the job requirements and desired qualifications and skills needed for the job and where job seekers can upload resumes and apply directly for the various vacancies through the website.  As a result, it does allow tracking of applications, reviews and job advertisements over time.  The geographic scope is currently primarily limited to greater Cairo and Alexandria.  While the scope of these jobs are more limited they provide a picture of the skill and educational requirements that are desired for some of the top private sector jobs in Egypt. 

## Installation instructions

### Step 1

As the data being gathered are being stored in an SQLite database the user should first set up the databases by running OLXDatabaseConversion.py and WuzzufDatabaseConversion.py and ensuring that the function reset_tables() is uncommented.  This code can also be used to update or replace tables as needed and report key statistics from the data that has been inserted in the various tables.

### Step 2

The main code that scrapes the websites is scrape_Wuzzuf_cloudv2.py and analyze_Wuzzuf_cloudv2.py.  This code is designed to scrape the websites at daily intervals through a UNIX/LINUX based system where you set a crontab that runs the code once daily.  The code scrapes each page on day 1, and at weekly intervals thereafter until the job ad expires or ceases to exist.  This allows for some moderate tracking of job ad views on OLX.com and applications to different job ads on the Wuzzuf site over time.

## Analysis

# analyze_OLX.py

This code draws in the data from the SQL database and does some basic cleaning and translation from Arabic to English using googletrans.  At the moment it simply outputs some key summary statistics by job sector such as the share of managerial versus entry level jobs, whether a bachelor's education is desired, and whether the job is full-time based on the processing of the data.  The analysis is not yet optimized to take advantage of the potential time series nature of the dataset.

# analyze_wuzzuf.py

Cleans the scraped dataset to turn it into a dataset that is ready for analysis.  This dataset also performs some basic tagging of words to identify the key skills in demand associated with various occupations.  It is visualized through word clouds.  Initial findings show that many of 

## Future Improvements

The aim is to continue to improve the analytics behind the data gathered from each of these websites to turn it into more real-time information that can be used for policy decisions and help potential job seekers and students in Egypt to have critical and timely information on labor market and job demands that can help inform educational and skill investments that improve the potential for these job seekers to enter better jobs.
