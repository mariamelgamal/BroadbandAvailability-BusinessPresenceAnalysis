#!/usr/bin/env python
import sys
import csv

columns = ["STATE","SDSCR","COUNTY","CTYDSCR","NAICS","NCSDSCR","CESTAB", "CBP_PCT","NES_PCT","EST","EMP","EMP_NF","QP1","QP1_NF","AP","AP_NF","ESTAB","RCPTOT","RCPTOT_N_F"]
NAICS_dic = {
	'11': "Agriculture Forestry Fishing and Hunting",
	'21': "Mining",
	'22': "Utilities",
	'23': "Construction",
	'31-33': "Manufacturing",
	'42': "Wholesale Trade",
	'44': "Retail Trade",
	'45': "Retail Trade",
	'48': "Transportationa and Warehousing",
	'49': "Transportationa and Warehousing",
	'51': "Information",
	'52': "Finance and Insurance",
	'53': "Real Estate Rental and Leasing",
	'54': "Professional Scientific and Technical Services",
	'55': "Management of Companies and Enterprises",
	'56': "Administrative and Support and Waste Management and Remediation Services",
	'61': "Educational Services",
	'62': "Health Care and Social Assistance",
	'71': "Arts Entertainment and Recreation",
	'72': "Accomodation and Food Services",
	'81': "Other Services Except Public Administration",
	'92': "Public Administration"
}
# open file and read each line
reader = csv.DictReader(sys.stdin, delimiter=',', fieldnames = columns)
#next(reader)
for row in reader:
	state = row["STATE"].zfill(2)
	county = row["COUNTY"].zfill(3)
	stcnty = state + county
	NAICS = row["NAICS"]
	Industry_dsc = row["NCSDSCR"].replace(',', '')
	if ((str(state) != '00') and (str(county) != '000') and (str(NAICS) != '0') and (str(county)!='999')):
		key = stcnty
		if((str(NAICS)[:2]) in NAICS_dic):
			Industry = NAICS_dic[(str(NAICS)[:2])]
			val = row["SDSCR"]+ ", " + row["CTYDSCR"] + ", " + NAICS + ", " + Industry + ", " + Industry_dsc + ", " + row["EMP"] + ", " \
			+ row["EMP_NF"] + ", " + row["AP"] + ", " + row["AP_NF"] + ", " + row["RCPTOT"] + ", " + row["RCPTOT_N_F"]
	    		print("%s \t %s" % (key, val))
