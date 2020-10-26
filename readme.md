# Exploring Broadband Availability and Business Presence in the US

Mariam Elgamal, 
Simran Parwani 

### Abstract
The digital divide in the United States, marked by unequal access to the internet, is especially relevant now with an increasingly digital economy and physical distancing practices. Our project explores the link between broadband infrastructure at the census block level with the economic presence of businesses in that region. Using Hadoop MapReduce and Apache Impala, we have uncovered major discrepancies in federal broadband data collection and found that counties with higher average broadband speeds tended to have higher payroll for a similar geographic composition. We further evaluated the discrepancies with tested broadband speeds and uncovered a digital divide case study based on rural-urban inequality.

### Contents
/ana_code
* profiling.sql 


/data_ingest
* data_transfer.sh: contains commands for creating HDFS directories for data
* seattle_speed.tsv (refer to data section below)
* ruralurban.tsv (refer to data section below)

/etl_code
* Mapper.py: the mapper program for cleaning the broadband deployment data from the Federal Communications Commission. The schema has been modified to now include the state abbreviation, the county code created key, the FIPS census block code, the residential access, the residential downstream speed, the residential upstream speed, the business access, the business upstream speed, and the business downstream speed.

* Reducer.py: the reducer program for cleaning the broadband deployment data from the Federal Communications Commission

* CBPmodMapper.py: the mapper program for cleaning the County Business Patterns data. We have modified the schema to include the state and county code, state name, county name, NAICS code, the industry name, the NAICS description, the employment range, the employment noise flag (stability), annual payroll, and annual payroll noise flag (stability). We also created a key combining the state and county code which matches the first five digits of the FIPS census block code in the broadband dataset giving us a way to join the datasets.

* CleanReducer.py: the reducer program for cleaning the County Business Patterns data

* cbpdata_clean.sql: commands for CBP data cleaning


/profiling_code
* profiling.sql: contains over 540 Impala commands exploring the code.

/screenshots
* contains screenshots of impala commands

### Obtaining the data
Federal Communications Commission Fixed Broadband Deployment Data  |  2015, 2016, 2017
For each census block, internet providers report the maximum advertised upstream and downstream speeds. This dataset is the only data available on broadband availability in the US.
11 GB x 3 years  |  [Get FCC Data] (https://www.fcc.gov/general/broadband-deployment-data-fcc-form-477)

US Census Bureau County Business Patterns  |  2015, 2016, 2017
This dataset shows every business in every county. It shows the annual payroll, the employment range, the industry classification, and the receipts for non-paid employees.
180 MB x 3 years  |  [Get CBP Data] (https://www.census.gov/programs-surveys/cbp/data/datasets.html)

US Department of Agriculture Rural Urban Continuum Codes  |  2013 (last available dataset)
Each county is given a code from 1-9 indicating its population size and degree of urbanization/proximity to metropolitan area. The dataset is updated every 10 years.
769 KB  |  [Get RUCC data] (https://www.ers.usda.gov/data-products/rural-urban-continuum-codes.aspx)
*Note that we converted to tsv before transferring to HDFS as these were merely secondary data sources. Since the file is small, we have included it in the data_ingest directory to make it easier for the graders.

Seattle Broadband Speed Test  |  updated periodically (we used the data from 2016)
This is a dataset from the City of Seattle hosted on Kaggle with the results of broadband speed tests in King County, Washington. We checked this against the FCC data.
463 KB  |  [Get Seattle Broadband Speed Test data] (https://www.kaggle.com/city-of-seattle/seattle-broadband-speed-test/data)
*Note that we converted to tsv before transferring to HDFS as these were merely secondary data sources. Since the file is small, we have included it in the data_ingest directory to make it easier for the graders.



### Transfer to HDFS
1. Data for CBP was renamed to combine17.txt, combine16.txt, combine15.txt for each year respectively.
2. Data for FCC was renamed to broadband2017.csv, broadband2016.csv, broadband2015.csv for each year respectively.
3. Use commands in /data_ingest/data_transfer.sh to move data into HDFS directories.
4. Run Map Reduce programs using the files in the /etl_code directory above. Note that the .sh files in this directory transfer the clean data to a directory in your HDFS.




### Data Profiling in Impala
1. Be sure to have all data files in their own directory in your user directory in HDFS.
2. Replace your netid and directory for all locations specified in create table commands in the file profiling.sql. Example below.

```
drop table if exists cbp16;
create external table cbp16 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business16/';
```

3. Transfer profiling.sql file to your user directory in HDFS.

4. If using DUMBO to access Impala, use the following commands to connect after logging into DUMBO.

```
impala-shell
connect compute-1-1;
use <YOUR NETID>;
```

5. Run profiling.sql file in impala using the source command from inside the impala-shell.

```
source profiling.sql;
```


### Building Analytic in Impala
1. Follow steps 1-3 above for section "Data Profiling in Impala"
2. Run analytic.sql file in impala using the source command from inside the impala shell.

```
source analytic.sql
```



