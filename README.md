# EgyptJobData

This repository scrapes several job websites in Egypt to gather information on labor market demands.  This information is potentially useful for tracking occupational trends over time, informing educational investments and the design of curriculums that can help improve labor market outcomes in Egypt.  

## Background and Problem Statement

In Egypt and many developing countries the data that is available to policy makers and to individuals to understand how and where to invest in their education is limited.  Having this information is especially critical in an environment where it is estimated that nearly three-quarters of the the people of working age were out of the labor force, unemployed, informally employed or not-well matched their jobs in 2014 based on the Egypt Labour Force Survey.  Unemployment is particularly severe among many university graduates and even those with STEM type degrees.  Among the university educated population, unemployment stood at over 20 percent of the population for most degrees with the exception of medical.  This could be reflective of an education system that has faced challenges in developing the critical skills that graduates need to be successful in the labor market particularly because of gaps in information to guide decision making.

## Websites Scraped

The two repositories that are scraped are:

1. OLX.com
2. Wuzzuf.net

OLX.com contains nearly 130K job ads at any given time with around one-third of these ads applying to job seekers and the other two-thirds of job ads applying to open job vacancies.  Most of these job ads are listed in Arabic and therefore capture a good deal of the local market.  The geographic coverage of these ads is extensive covering nearly 365 regions and all 27 governates of Egypt. Each job ad goes live for approximately 90 days or until removal by the user.  The extent to which these ads are potentially valid and truely representative of real job vacancies requires further investigation.  Frequency and extent to job postings may also be somewhat correlated with the availability and accessibility of the internet in different areas nevertheless it was assessed as one of the better sources for gathering job information online in the context of Egypt.

Wuzzuf.net is a job platform that is targeted at export oriented jobs and jobs in foreign companies.  It contains about 5K job ads at any given time with job ads typically expiring after 30 days.  Most job ads are listed in English.  The Wuzzuf is a professionalized platform where employers with job vacancies fill in many details about the job requirements and desired qualifications and skills needed for the job and where job seekers can upload resumes and apply directly for the various vacancies through the website.  As a result, it does allow tracking of applications, number of resumes reviewed and number of shortlisted candidates that apply to the job ads over time.  The geographic scope is currently primarily limited to greater Cairo and Alexandria.  While the scope of these jobs are more limited they provide a picture of the skill and educational requirements that are desired for some of the top private and non-governmental sector jobs in Egypt.

## Installation instructions

### Step 1

The data being gathered is intended to be stored in a SQLite database.  As a result, the user should creat the relevant databases by running OLXDatabaseConversion.py and WuzzufDatabaseConversion.py and ensuring that the function reset_tables() is uncommented.  This code can also be used to update or replace tables as needed and report key statistics from the data that has been inserted in the various tables.

### Step 2

The main code that scrapes the websites is scrapeEgyptOLX_cloudv2.py and scrapeWuzzuf_cloudv2.py.  This code is designed to scrape the websites at daily intervals through a UNIX/LINUX based system where you set a crontab that runs the code once daily.  The code scrapes each page on day 1, and at weekly intervals thereafter until the job ad expires or ceases to exist.  This allows for some moderate tracking of job ad views on OLX.com and applications to different job ads on the Wuzzuf site over time.

## Analysis

### analyzeOLX_v2.py

This code draws in the data from the SQL database and does some basic cleaning and translation from Arabic to English using googletrans.  At the moment it simply outputs key summary statistics by job sector such as the share of managerial versus entry level jobs, whether a bachelor's education is desired, and whether the job is full-time based on the processing of the data.  The analysis is not yet optimized to take advantage of the potential time series nature of the dataset.

### analyzeWuzzuf_v2.py

This code cleans the Wuzzuf data that is stored in the SQL database to turn it into a dataset that is ready for basic analysis.  This dataset also performs some basic tagging of words to identify the key skills in demand associated with various occupations by throwing out various stop words.  Skill demands are initially visualized through word clouds (see AnalyzeWuzzuf.py).

## Future Improvements

The aim is to continue to improve the analytics behind the data gathered from each of these websites to turn it into more real-time information that can be used for policy decisions and help potential job seekers and students in Egypt to have critical and timely information on labor market and job demands that can help inform educational and skill investments that improve the potential for these job seekers to enter better jobs.  The aim would be to build a dashboard that can link to some of the cleaned data that is being gathered from these job websites.
