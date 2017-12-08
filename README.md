# EgyptJobData

This repository scrapes several job websites in Egypt to gather information on labor market demands.  This information can be useful for tracking occupational trends over time, informing educational investments and the design of curriculums that can help improve labor market outcomes.  The two repositories that are scraped are:
1. OLX.com
2. Wuzzuf.net

#### scrape_Wuzzuf

Exports scraped data pages to csv with minimal data cleaning

#### analyze_Wuzzuf

Cleans the scraped dataset to turn it into a dataset that is ready for analysis.  This dataset also performs some basic tagging of words to identify the key skills in demand associated with various occupations.  It is visualized through word clouds.

#### scrape_OLX

Exports scraped data pages to csv file.  The first part focuses simply on getting sectoral counts of job ads, while the second part of the code scrapes the individual job ad pages. This code is still in the process of being developed and finalized.

### Future Improvements

1. Speed and efficiency improvements in scraping.  Currently OLX code takes a few days to scrape all of the websites, while Wuzzuf takes a few hours.

2. Automating the scheduling of downloads to occur at least once per month.

3. Tracking time length that job ad vacancies are posted.  Challenge is that there could be re-advertisements of postings if the employer cannot find prospective candidates within the time length that the advertisement is valid.
