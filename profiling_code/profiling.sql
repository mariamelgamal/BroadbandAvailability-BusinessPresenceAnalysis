--!/bin/bash

--Impala-shell
--connect compute-1-1; 
--use net-ID;


--creating cbp tables

--create cbp 2015 table 
drop table if exists cbp15;
create external table cbp15 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business15/';

--testing out impala by finding number of businesses in a county
drop table if exists cbp15_numBusinesses;
create external table cbp15_numBusinesses (stcnty_code string, num_businesses BIGINT);

insert into table cbp15_numBusinesses select lpad(stcnty_code,5,"0") as county_code, count(stcnty_code) as num_businesses from cbp15 group by lpad(stcnty_code,5,"0") order by lpad(stcnty_code,5,"0");

select * from cbp15_numbusinesses limit 20;

--create cbp 2016 table 
drop table if exists cbp16;
create external table cbp16 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business16/';

drop table if exists cbp16_numBusinesses;
create external table cbp16_numBusinesses (stcnty_code string, num_businesses BIGINT);

insert into table cbp16_numBusinesses select lpad(stcnty_code,5,"0") as county_code, count(stcnty_code) as num_businesses from cbp16 group by lpad(stcnty_code,5,"0") order by lpad(stcnty_code,5,"0");

select * from cbp16_numbusinesses limit 20;

--create cbp 2017 table 
drop table if exists cbp17;
create external table cbp17 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business/';
drop table if exists cbp17_numBusinesses;
create external table cbp17_numBusinesses (stcnty_code string, num_businesses BIGINT);

insert into table cbp17_numBusinesses select lpad(stcnty_code,5,"0") as county_code, count(stcnty_code) as num_businesses from cbp17 group by lpad(stcnty_code,5,"0") order by lpad(stcnty_code,5,"0");

select * from cbp17_numbusinesses limit 20;

--calculating number of industries per county
--calculating number of industries per county for year 2015

drop table if exists cbp15_numIndustriesPerCounty;
create external table cbp15_numIndustriesPerCounty (stcnty_code string, industry string, num_industry_per_county BIGINT);

insert into table cbp15_numIndustriesPerCounty select  lpad(stcnty_code,5,"0") as county_code, industry as industry, count(industry) as num_industry_per_county from cbp15 group by  lpad(stcnty_code,5,"0"), industry order by  lpad(stcnty_code,5,"0"), industry;

select * from cbp15_numIndustriesPerCounty limit 20;

--calculating number of industries per county for year 2016
drop table if exists cbp16_numIndustriesPerCounty;
create external table cbp16_numIndustriesPerCounty (stcnty_code string, industry string, num_industry_per_county BIGINT);

insert into table cbp16_numIndustriesPerCounty select  lpad(stcnty_code,5,"0") as county_code, industry as industry, count(industry) as num_industry_per_county from cbp16 group by  lpad(stcnty_code,5,"0"), industry order by  lpad(stcnty_code,5,"0"), industry;

select * from cbp16_numIndustriesPerCounty limit 20;

--calculating number of industries per county for year 2017
drop table if exists cbp17_numIndustriesPerCounty;
create external table cbp17_numIndustriesPerCounty (stcnty_code string, industry string, num_industry_per_county BIGINT);

insert into table cbp17_numIndustriesPerCounty select  lpad(stcnty_code,5,"0") as county_code, industry as industry, count(industry) as num_industry_per_county from cbp17 group by  lpad(stcnty_code,5,"0"), industry order by  lpad(stcnty_code,5,"0"), industry;

select * from cbp17_numIndustriesPerCounty limit 20;


-- creating fcc tables
drop table if exists temp15;
create external table temp15 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string) row format delimited fields terminated by '\t' location '/user/sp4494/broadband2015clean_data/';

drop table if exists fcc15;
create external table fcc15 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string);

drop table if exists temp16;
create external table temp16 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string) row format delimited fields terminated by '\t' location '/user/sp4494/broadband2016clean_data/';
drop table if exists fcc16;
create external table fcc16 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string);

insert into fcc16 select * from temp16 where length(state) = 2 and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' ;

insert into fcc15 select * from temp15 where length(state) = 2 and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' ;

drop table if exists fcc17;
create external table fcc17 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string) row format delimited fields terminated by '\t' location '/user/sp4494/broadband_data/';


-- Percentage of blocks in each county with internet

drop table if exists pct_internet17;
create external table pct_internet17 (county_code string, pct_with_internet double);

insert into pct_internet17 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc17 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

drop table if exists pct_internet16;
create external table pct_internet16 (county_code string, pct_with_internet double);

insert into pct_internet16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc16 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

drop table if exists pct_internet15;
create external table pct_internet15 (county_code string, pct_with_internet double);

insert into pct_internet15 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc15 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;



-- Avg Upstream and Downstream speeds per county
drop table if exists avg_speed17;
create external table avg_speed17 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into avg_speed17 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists avg_speed16;
create external table avg_speed16 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into avg_speed16 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists avg_speed15;
create external table avg_speed15 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into avg_speed15 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

--Select Max Downstream and Upstream
drop table if exists max_speed17;
create external table max_speed17 (county_code string, max_downstream_speed double, max_upstream_speed double);

insert into max_speed17 select county_code, max(cast(max_cir_down as float)) max_downstream_speed, max(cast(max_cir_up as float)) max_upstream_speed from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists max_speed16;
create external table max_speed16 (county_code string, max_downstream_speed double, max_upstream_speed double);

insert into max_speed16 select county_code, max(cast(max_cir_down as float)) max_downstream_speed, max(cast(max_cir_up as float)) max_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists max_speed15;
create external table max_speed15 (county_code string, max_downstream_speed double, max_upstream_speed double);

insert into max_speed15 select county_code, max(cast(max_cir_down as float)) max_downstream_speed, max(cast(max_cir_up as float)) max_upstream_speed from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;



--select Min Downstream and Upstream
drop table if exists min_speed17;
create external table min_speed17 (county_code string, min_downstream_speed double, min_upstream_speed double);

insert into min_speed17 select county_code, min(cast(max_cir_down as float)) min_downstream_speed, min(cast(max_cir_up as float)) min_upstream_speed from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists min_speed16;
create external table min_speed16 (county_code string, min_downstream_speed double, min_upstream_speed double);

insert into min_speed16 select county_code, min(cast(max_cir_down as float)) min_downstream_speed, min(cast(max_cir_up as float)) min_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists min_speed15;
create external table min_speed15 (county_code string, min_downstream_speed double, min_upstream_speed double);

insert into min_speed15 select county_code, min(cast(max_cir_down as float)) min_downstream_speed, min(cast(max_cir_up as float)) min_upstream_speed from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;


--combining average broadband speed over years (2015, 2016, 2017)
drop table if exists avg_speed_all;
create table avg_speed_all (county_code string, avg_up2015 double, avg_down2015 double, avg_up2016 double, avg_down2016 double, avg_up2017 double, avg_down2017 double);

insert into avg_speed_all select distinct a.county_code, a.avg_upstream_speed, a.avg_downstream_speed, b.avg_upstream_speed, b.avg_downstream_speed,  c.avg_upstream_speed, c.avg_downstream_speed from avg_speed15 c inner join avg_speed16 b on c.county_code = b.county_code inner join avg_speed17 a on a.county_code = b.county_code;


--calculating percent change of average upstream and downstream speed between years
drop table if exists avg_speed_change;
create table avg_speed_change (county_code string, change_avg_up1516 double, change_avg_down1516 double, change_avg_up1617 double, change_avg_down1617 double);

insert into avg_speed_change select county_code, (avg_up2016 - avg_up2015) /  avg_up2015 as change_up_speed1516,  (avg_up2017 - avg_up2016) /  avg_up2016 as change_up_speed1617, (avg_down2016 - avg_down2015) /  avg_down2015 as change_down_speed1516,  (avg_up2017 - avg_up2016) /  avg_up2016 as change_down_speed1617  from avg_speed_all;


--combining maximum speed over years
drop table if exists max_speed_all;
create table max_speed_all (county_code string, max_up2015 double, max_down2015 double, max_up2016 double, max_down2016 double, max_up2017 double, max_down2017 double);

insert into max_speed_all select distinct a.county_code, a.max_upstream_speed, a.max_downstream_speed, b.max_upstream_speed, b.max_downstream_speed,  c.max_upstream_speed, c.max_downstream_speed from max_speed15 c inner join max_speed16 b on c.county_code = b.county_code inner join max_speed17 a on a.county_code = b.county_code;


-- calculating percent change of maximum upstream and downstream speed between years
drop table if exists max_speed_change;
create table max_speed_change (county_code string, change_max_up1516 double, change_max_down1516 double, change_max_up1617 double, change_max_down1617 double);

insert into max_speed_change select county_code, (max_up2016 - max_up2015) /  max_up2015 as change_up_speed1516,  (max_up2017 - max_up2016) /  max_up2016 as change_up_speed1617, (max_down2016 - max_down2015) /  max_down2015 as change_down_speed1516,  (max_up2017 - max_up2016) /  max_up2016 as change_down_speed1617  from max_speed_all;


--combining minimum broadband speeds over years
drop table if exists min_speed_all;
create table min_speed_all (county_code string, min_up2015 double, min_down2015 double, min_up2016 double, min_down2016 double, min_up2017 double, min_down2017 double);

insert into min_speed_all select distinct a.county_code, a.min_upstream_speed, a.min_downstream_speed, b.min_upstream_speed, b.min_downstream_speed,  c.min_upstream_speed, c.min_downstream_speed from min_speed15 c inner join min_speed16 b on c.county_code = b.county_code inner join min_speed17 a on a.county_code = b.county_code;


-- calculating percent change of minimum upstream and downstream speed between years
drop table if exists min_speed_change;
create table min_speed_change (county_code string, change_min_up1516 double, change_min_down1516 double, change_min_up1617 double, change_min_down1617 double);

insert into min_speed_change select county_code, (min_up2016 - min_up2015) /  min_up2015 as change_up_speed1516,  (min_up2017 - min_up2016) /  min_up2016 as change_up_speed1617, (min_down2016 - min_down2015) /  min_down2015 as change_down_speed1516,  (min_up2017 - min_up2016) /  min_up2016 as change_down_speed1617  from min_speed_all;

--combining percentage of county with commercial internet access over years
drop table if exists pct_with_internet_all;
create external table pct_with_internet_all (county_code string, pct2015 double, pct2016 double, pct2017 double);

insert into pct_with_internet_all select distinct a.county_code, a.pct_with_internet, b.pct_with_internet, c.pct_with_internet from pct_internet15 c inner join pct_internet16 b on c.county_code = b.county_code inner join pct_internet17 a on a.county_code = b.county_code;


--calculating percent change of percentage of county with commercial internet access between years

drop table if exists pct_with_internet_change;
create external table pct_with_internet_change (county_code string, change_pct1516 double, change_pct1617 double);

insert into pct_with_internet_change select county_code, (pct2016 - pct2015) /  pct2015 as change1516, (pct2017 - pct2016) /  pct2016 as change1617 from pct_with_internet_all;

select p.change_pct1516 - pp.avg_pct from pct_with_internet_change p cross join (select avg(change_pct1516) as avg_pct from pct_with_internet_change) pp;



--average annual payroll per industry per county
--average annual payroll per industry per county for year 2017

drop table if exists cbp17_averagepayroll;
create external table cbp17_averagepayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp17_averagepayroll select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp17 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;

select * from cbp17_averagepayroll limit 20;

--average annual payroll per industry per county for year 2016
drop table if exists cbp16_avgpayroll; 
create external table cbp16_avgpayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp16_avgpayroll select (CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2))) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp16 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;

select * from cbp16_avgpayroll limit 20;

--average annual payroll per industry per county for year 2015
drop table if exists cbp15_avgpayroll;
create external table cbp15_avgpayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp15_avgpayroll select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp15 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;

select * from cbp15_avgpayroll limit 20;


--combine average payroll over the years
drop table if exists all_avgpayroll;
create external table all_avgpayroll(stcnty_naics string, avg_payroll2015 double, avg_payroll2016 double, avg_payroll2017 double);

insert into all_avgpayroll select distinct a.stcnty_naics, a.avg_payroll_per_industry_per_county, b.avg_payroll_per_industry_per_county, c.avg_payroll_per_industry_per_county from cbp15_avgpayroll c inner join cbp16_avgpayroll b on c.stcnty_naics = b.stcnty_naics inner join cbp17_averagepayroll a on a.stcnty_naics = b.stcnty_naics;

select * from all_avgpayroll limit 20;


-- calculating percent change of average annual payroll over the years (2015, 2016, 2017)
drop table if exists all_avgpayroll_change;
create external table all_avgpayroll_change (stcnty_naics string, avg_payroll_change1516 double, avg_payroll_change1617 double);

insert into all_avgpayroll_change select stcnty_naics, (avg_payroll2016 - avg_payroll2015) /  avg_payroll2015 as change_avg_payroll1516,  (avg_payroll2017 - avg_payroll2016) /  avg_payroll2016 as change_avg_payroll1617 from all_avgpayroll;

select * from all_avgpayroll_change limit 20;


-- average NES receipts per industry per county
-- average NES receipts per industry per county for year 2017
drop table if exists cbp17_avgnesreceipts;
create external table cbp17_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp17_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp17 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;

select * from cbp17_avgnesreceipts limit 20;

-- average NES receipts per industry per county for year 2016
drop table if exists cbp16_avgnesreceipts;

create external table cbp16_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp16_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp16 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;

select * from cbp16_avgnesreceipts limit 20;

-- average NES receipts per industry per county for year 2015
drop table if exists cbp15_avgnesreceipts;

create external table cbp15_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp15_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp15 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;

select * from cbp15_avgnesreceipts limit 20;

--Combine Average NES Receipts over the years (2015, 2016, 2017)
drop table if exists all_avgnesreceipts;

create external table all_avgnesreceipts(stcnty_naics string, avg_nesreceipts2015 double, avg_nesreceipts2016 double, avg_nesreceipts2017 double);

insert into all_avgnesreceipts select distinct a.stcnty_naics, a.avg_nes_per_industry_per_county, b.avg_nes_per_industry_per_county, c.avg_nes_per_industry_per_county from cbp15_avgnesreceipts c inner join cbp16_avgnesreceipts b on c.stcnty_naics = b.stcnty_naics inner join cbp17_avgnesreceipts a on a.stcnty_naics = b.stcnty_naics;

select * from all_avgnesreceipts limit 20;


-- Calculating Percent Change of Average Annual NES Receipts Over the Years
drop table if exists all_avgnesreceipts_change;

create external table all_avgnesreceipts_change (stcnty_naics string, avg_nes_change1516 double, avg_nes_change1617 double);

insert into all_avgnesreceipts_change select stcnty_naics, (avg_nesreceipts2016 - avg_nesreceipts2015) /  avg_nesreceipts2015 as change_avg_nesreceipts1516,  (avg_nesreceipts2017 - avg_nesreceipts2016) /  avg_nesreceipts2016 as change_avg_nesreceipts1617 from all_avgnesreceipts;

select * from all_avgnesreceipts_change limit 20;



--Combine all number of businesses per county over the years
drop table if exists all_numbusinesses;
create external table all_numbusinesses(stcnty string, numbusineses2015 double, numbusinesses2016 double, numbusinesses2017 double);

insert into all_numbusinesses select distinct a.stcnty_code, a.num_businesses, b.num_businesses, c.num_businesses from cbp15_numbusinesses c inner join cbp16_numbusinesses b on c.stcnty_code = b.stcnty_code inner join cbp17_numbusinesses a on a.stcnty_code = b.stcnty_code;

select * from all_numbusinesses limit 20;


-- Create Agriculture table
drop table if exists agriculture_payroll;
create table agriculture_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into agriculture_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '11' and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;

select * from agriculture_payroll limit 10;


-- Create Education table
drop table if exists education_payroll;
create external table education_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into education_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '61' and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;

select * from education_payroll limit 10;



-- Create Retail table
drop table if exists retail_payroll;
create external table retail_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into retail_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '44' and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;


select * from retail_payroll limit 10;


-- Create Arts table
drop table if exists art_payroll;
create table art_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into art_payroll select strleft(stcnty_naics, 5), stcnty_naics,  avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '71'  and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;

select * from art_payroll limit 10;


-- Create Professional table
drop table if exists prof_payroll;
create table prof_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into prof_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '54'  and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;

select * from prof_payroll limit 10;



-- retail and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- retail and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 as x, avg(f.pct2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 as x, avg(f.pct2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from agriculture_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and minimum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.min_up2015 as x, avg(f.min_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from retail_payroll a inner join min_speed_all f on a.stcnty = f.county_code) ols;

-- education and minimum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.min_down2015 as x, avg(f.min_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join min_speed_all f on a.stcnty = f.county_code) ols;



-- education and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from education_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from art_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.payroll15 as y, avg(a.payroll15) over () as y_bar from prof_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- retail and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- retail and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from retail_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from agriculture_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from education_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from art_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.payroll16 as y, avg(a.payroll16) over () as y_bar from prof_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- retail and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from retail_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;



-- retail and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from retail_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from retail_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from agriculture_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from agriculture_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from agriculture_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from education_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from education_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from education_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from art_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from art_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from art_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from prof_payroll a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from prof_payroll a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.payroll17 as y, avg(a.payroll17) over () as y_bar from prof_payroll a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

--NES

-- Create Agriculture table
drop table if exists agriculture_nes;
create table agriculture_nes (stcnty string, stcnty_naics string, nes15 double, nes16 double, nes17 double);

insert into agriculture_nes select strleft(stcnty_naics, 5), stcnty_naics, avg_nesreceipts2015, avg_nesreceipts2016, avg_nesreceipts2017 from all_avgnesreceipts where strright(stcnty_naics, 2) = '11' and avg_nesreceipts2015 is not null and avg_nesreceipts2016 is not null and avg_nesreceipts2017 is not null;

select * from agriculture_nes limit 10;


-- Create Education table
drop table if exists education_nes;
create table education_nes (stcnty string, stcnty_naics string, nes15 double, nes16 double, nes17 double);

insert into education_nes select strleft(stcnty_naics, 5), stcnty_naics, avg_nesreceipts2015, avg_nesreceipts2016, avg_nesreceipts2017 from all_avgnesreceipts where strright(stcnty_naics, 2) = '61' and avg_nesreceipts2015 is not null and avg_nesreceipts2016 is not null and avg_nesreceipts2017 is not null;

select * from education_nes limit 10;




-- Create Retail table
drop table if exists retail_nes;
create table retail_nes (stcnty string, stcnty_naics string, nes15 double, nes16 double, nes17 double);

insert into retail_nes select strleft(stcnty_naics, 5), stcnty_naics, avg_nesreceipts2015, avg_nesreceipts2016, avg_nesreceipts2017 from all_avgnesreceipts where strright(stcnty_naics, 2) = '44' and avg_nesreceipts2015 is not null and avg_nesreceipts2016 is not null and avg_nesreceipts2017 is not null;

select * from retail_nes limit 10;



-- Create art table
drop table if exists art_nes;
create table art_nes (stcnty string, stcnty_naics string, nes15 double, nes16 double, nes17 double);

insert into art_nes select strleft(stcnty_naics, 5), stcnty_naics, avg_nesreceipts2015, avg_nesreceipts2016, avg_nesreceipts2017 from all_avgnesreceipts where strright(stcnty_naics, 2) = '71' and avg_nesreceipts2015 is not null and avg_nesreceipts2016 is not null and avg_nesreceipts2017 is not null;

select * from art_nes limit 10;


-- Create professional table
drop table if exists prof_nes;
create table prof_nes (stcnty string, stcnty_naics string, nes15 double, nes16 double, nes17 double);

insert into prof_nes select strleft(stcnty_naics, 5), stcnty_naics, avg_nesreceipts2015, avg_nesreceipts2016, avg_nesreceipts2017 from all_avgnesreceipts where strright(stcnty_naics, 2) = '54' and avg_nesreceipts2015 is not null and avg_nesreceipts2016 is not null and avg_nesreceipts2017 is not null;

select * from prof_nes limit 10;

-- retail and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- retail and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 as x, avg(f.pct2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from retail_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 as x, avg(f.pct2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from agriculture_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from education_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from art_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2015 as x, avg(f.max_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2015 as x, avg(f.max_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2015 as x, avg(f.avg_up2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2015 as x, avg(f.avg_down2015) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2015
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2015 * 100 as x, avg(f.pct2015 * 100) over () as x_bar, a.nes15 as y, avg(a.nes15) over () as y_bar from prof_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- retail and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from retail_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 as x, avg(f.pct2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from agriculture_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- education and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 * 100 as x, avg(f.pct2016 * 100) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from education_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 * 100 as x, avg(f.pct2016 * 100) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from art_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2016 as x, avg(f.max_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2016 as x, avg(f.max_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2016 as x, avg(f.avg_up2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2016 as x, avg(f.avg_down2016) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2016 * 100 as x, avg(f.pct2016 * 100) over () as x_bar, a.nes16 as y, avg(a.nes16) over () as y_bar from prof_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;


-- retail and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- retail and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- retail and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- retail and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;





-- agriculture and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from agriculture_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- agriculture and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from agriculture_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- agriculture and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 as x, avg(f.pct2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from agriculture_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- education and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- education and minimum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select f.min_up2017 as x, avg(f.min_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from retail_nes a inner join min_speed_all f on a.stcnty = f.county_code) ols;

-- education and minimum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.min_down2017 as x, avg(f.min_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join min_speed_all f on a.stcnty = f.county_code) ols;



-- education and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- education and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 * 100 as x, avg(f.pct2017 * 100) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from education_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;



-- art and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- art and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from art_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- art and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from art_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- art and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 * 100 as x, avg(f.pct2017 * 100) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from art_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;

-- professional and maximum upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_up2017 as x, avg(f.max_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;

-- professional and maximum downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.max_down2017 as x, avg(f.max_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from prof_nes a inner join max_speed_all f on a.stcnty = f.county_code) ols;


-- professional and average upstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_up2017 as x, avg(f.avg_up2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and average downstream speed 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.avg_down2017 as x, avg(f.avg_down2017) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from prof_nes a inner join avg_speed_all f on a.stcnty = f.county_code) ols;

-- professional and Percentage of County with Internet Access 2017
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar))) as slope from (select f.pct2017 * 100 as x, avg(f.pct2017 * 100) over () as x_bar, a.nes17 as y, avg(a.nes17) over () as y_bar from prof_nes a inner join pct_with_internet_all f on a.stcnty = f.county_code) ols;


--Exploring percentage of data with commercial internet marked as '1' yet have internet speeds as '0'

--count number of faulty records for year 2017
with faulty_rec as (select count(*) as faulty from fcc17 where business ='1'  and max_cir_up='0' and max_cir_down='0') select f.faulty/t.total from faulty_rec f cross join (select count(*) as total from fcc17) t;

--count number of faulty records for year 2016
with faulty_rec as (select count(*) as faulty from fcc16 where business ='1' and max_cir_up='0' and max_cir_down='0') select f.faulty/t.total from faulty_rec f cross join (select count(*) as total from fcc16) t;

--count number of faulty records for year 2015
with faulty_rec as (select count(*) as faulty from fcc15 where business ='1' and max_cir_up='0' and max_cir_down='0') select f.faulty/t.total from faulty_rec f cross join (select count(*) as total from fcc15) t;

-- calculating percentage of data with faulty commercial internet flag for year 2017
drop table if exists filtered17;
create external table filtered17 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string);

insert into filtered17 select * from fcc17 where business ='1' and max_cir_up!='0' and max_cir_down!='0' union select * from fcc17 where business ='0' and max_cir_up='0' and max_cir_down='0';

drop table if exists filtered_pct17;
create external table filtered_pct17 (county_code string, pct_with_internet double);

insert into filtered_pct17 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from filtered17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from filtered17 where max_cir_up != '0' and max_cir_down != '0' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select distinct * from filtered_pct17 order by pct_with_internet limit 20;

-- calculating percentage of data with faulty commercial internet flag for year 2016
drop table if exists filtered16;
create external table filtered16 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string);

insert into filtered16 select * from fcc16 where business ='1' and max_cir_up!='0' and max_cir_down!='0' union select * from fcc16 where business ='0' and max_cir_up='0' and max_cir_down='0';

drop table if exists filtered_pct16;
create external table filtered_pct16 (county_code string, pct_with_internet double);

insert into filtered_pct16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from filtered16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from filtered16 where max_cir_up != '0' and max_cir_down != '0' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select distinct * from filtered_pct16 order by pct_with_internet limit 20;


-- calculating percentage of data with faulty commercial internet flag for year 2015
drop table if exists filtered15;
create external table filtered15 (state string, block_code string, county_code string, consumer string, max_ad_down string, max_ad_up string, business string, max_cir_down string, max_cir_up string);

insert into filtered15 select * from fcc15 where business ='1' and max_cir_up!='0' and max_cir_down!='0' union select * from fcc15 where business ='0' and max_cir_up='0' and max_cir_down='0';

drop table if exists filtered_pct15;
create external table filtered_pct15 (county_code string, pct_with_internet double);

insert into filtered_pct15 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from filtered16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from filtered15 where max_cir_up != '0' and max_cir_down != '0' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select distinct * from filtered_pct15 order by pct_with_internet limit 20;

--display states which don't have full broadband availability for year 2017
select distinct strleft(county_code, 2) from (select * from filtered_pct17 where pct_with_internet != 1) d;

--display states which have less than 50% broadband availability for year 2017
select distinct strleft(county_code, 2) from (select * from filtered_pct17 where pct_with_internet < .5) d;

--display states which have less than 5% broadband availability  for year 2017
select strleft(county_code, 2) as state, count(*) from (select * from filtered_pct17 where pct_with_internet < .05) d group by state order by count(*);

--display states which have less than 10% broadband availability for year 2017
select distinct strleft(county_code, 2) from (select * from filtered_pct17 where pct_with_internet < .1) d;

--combine filtered percentage of data with faulty commercial internet flag tables
--(2015, 2016, 2017)
drop table if exists filtered_pct_all;
create external table filtered_pct_all (county_code string, pct15 double, pct16 double, pct17 double);

insert into filtered_pct_all select a.county_code, a.pct_with_internet, b.pct_with_internet, c.pct_with_internet from filtered_pct15 a join filtered_pct16 b on a.county_code = b.county_code join filtered_pct17 c on b.county_code = c.county_code;

--display the combined filtered data for state county code '56039'
select stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where left(trim(stcnty_naics),5) = "56039";



--Creating rural urban continuum code table
drop table if exists rucc;
create external table rucc (county_code string, state string, county string, population string, ruralurban_code smallint, description string)  row format delimited fields terminated by '\t' location '/user/sp4494/ruralurban/' tblproperties('skip.header.line.count'='1');

select distinct p.*, r.ruralurban_code, r.description from filtered_pct_all p join rucc r on r.county_code = p.county_code where strleft(p.county_code, 2) = '39' order by p.county_code;


--Creating average speed tables excluding county blocks 
--with '1' in the access column but 0 in the speed columns
drop table if exists filtered_avgspeed17;
create external table filtered_avgspeed17 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into filtered_avgspeed17 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from filtered17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists filtered_avgspeed16;
create external table filtered_avgspeed16 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into filtered_avgspeed16 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from filtered16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;


drop table if exists filtered_avgspeed15;
create external table filtered_avgspeed15 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into filtered_avgspeed15 select county_code, avg(cast(max_cir_down as float)) avg_downstream_speed, avg(cast(max_cir_up as float)) avg_upstream_speed from filtered15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

drop table if exists filtered_avgspeed_all;
create table filtered_avgspeed_all (county_code string, avg_up2015 double, avg_down2015 double, avg_up2016 double, avg_down2016 double, avg_up2017 double, avg_down2017 double);

insert into filtered_avgspeed_all select distinct a.county_code, a.avg_upstream_speed, a.avg_downstream_speed, b.avg_upstream_speed, b.avg_downstream_speed,  c.avg_upstream_speed, c.avg_downstream_speed from filtered_avgspeed15 c inner join filtered_avgspeed16 b on c.county_code = b.county_code inner join filtered_avgspeed17 a on a.county_code = b.county_code;


-- creating a table exploring county blocks with speeds over 3 mbps
drop table if exists above_three16;
create external table above_three16 (county_code string, pct_over_3 double);

insert into above_three16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total left join (select count(distinct block_code) num_with_internet, county_code from fcc16 where max_cir_down > '3' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

--creating agriculture industry specific table comparing the percentage of counties with internet 
--speeds over 3 mbps with NES and Payroll and RUCC
drop table if exists agriculture2016;
create table agriculture2016 (stcnty string, pct_over_three double, agriculture_nes double, agriculture_payroll double, ruralurban_code string);

insert into agriculture2016 select a.county_code, a.pct_over_3, b.payroll16, c.nes16, CASE
    WHEN d.ruralurban_code < 4 THEN 'Metro'
    WHEN d.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN d.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN d.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS Ruralurban_continuum from above_three16 a left join agriculture_payroll b on a.county_code = b.stcnty join agriculture_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code;

select avg(agriculture_nes), avg(agriculture_payroll), ruralurban_code from agriculture2016 where pct_over_three > .8 group by ruralurban_code;

select * from agriculture2016 where pct_over_three > .8;

select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select pct_over_three as x, avg(pct_over_three) over () as x_bar, agriculture_payroll as y, avg(agriculture_payroll) over () as y_bar from agriculture2016) ols;


--creating retail industry specific table comparing the percentage of counties with internet 
--speeds over 3 mbps with NES and Payroll and RUCC

drop table if exists retail2016;
create table retail2016 (stcnty string, pct_over_three double,retail_nes double, retail_payroll double, ruralurban_code string);

insert into retail2016 select a.county_code, a.pct_over_3, b.payroll16, c.nes16, CASE
    WHEN d.ruralurban_code < 4 THEN 'Metro'
    WHEN d.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN d.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN d.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS Ruralurban_continuum from above_three16 a left join retail_payroll b on a.county_code = b.stcnty join retail_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code;

 select ruralurban_code, avg(pct_over_three), avg(retail_nes), avg(retail_payroll) from retail2016 where pct_over_three > .8 group by ruralurban_code;

select * from retail2016 order by stcnty;

select avg(percentage), ruralurban_code from (select a.county_code, a.pct_over_3 as percentage, CASE
    WHEN r.ruralurban_code < 4 THEN 'Metro'
    WHEN r.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN r.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN r.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_code from above_three16 a join rucc r on a.county_code = r.county_code) percent_rucc group by ruralurban_code;


select count(*) , CASE
    WHEN ruralurban_code < 4 THEN 'Metro'
    WHEN ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS category from rucc group by category;

--exploring impact of county blocks reporting 1 for access but having 0 for upstream and downstream speeds
drop table if exists filtered_above_three16;
create external table filtered_above_three16 (county_code string, pct_over_3 double);

insert into filtered_above_three16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from filtered16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total left join (select count(distinct block_code) num_with_internet, county_code from filtered16 where max_cir_down > '3' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select avg(percentage), ruralurban_code from (select a.county_code, a.pct_over_3 as percentage, CASE
    WHEN r.ruralurban_code < 4 THEN 'Metro'
    WHEN r.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN r.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN r.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_code from filtered_above_three16 a join rucc r on a.county_code = r.county_code) percent_rucc group by ruralurban_code;

--creating retail specific with RUCC 
drop table if exists retail2016_withcode;
create table retail2016_withcode (stcnty string, pct_over_three double,retail_nes double, retail_payroll double, ruralurban_code int);

insert into retail2016_withcode select a.county_code, a.pct_over_3, b.payroll16, c.nes16, d.ruralurban_code from above_three16 a left join retail_payroll b on a.county_code = b.stcnty join retail_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code;

--looking at retail industry in specific states
select avg(retail_nes), avg(pct_over_three), avg(retail_payroll), ruralurban_code from retail2016_withcode where strleft(stcnty, 2) = '46' group by ruralurban_code order by ruralurban_code;

select avg(retail_nes), avg(pct_over_three), avg(retail_payroll), ruralurban_code from retail2016_withcode where strleft(stcnty, 2) = '39' group by ruralurban_code order by ruralurban_code;

select avg(retail_nes), avg(pct_over_three), avg(retail_payroll), ruralurban_code from retail2016_withcode where strleft(stcnty, 2) = '48' group by ruralurban_code order by ruralurban_code;

select avg(retail_nes), avg(pct_over_three), avg(retail_payroll), ruralurban_code from retail2016_withcode where strleft(stcnty, 2) = '06' group by ruralurban_code order by ruralurban_code;

select avg(retail_nes), avg(pct_over_three), avg(retail_payroll), ruralurban_code from retail2016_withcode where strleft(stcnty, 2) = '36' group by ruralurban_code order by ruralurban_code;



--creating professional services industry specific table comparing the percentage of 
--counties with internet speeds over 3 mbps with NES and Payroll and RUCC

drop table if exists prof2016;
create table prof2016 (stcnty string, pct_over_three double, avg_speed double, max_speed double, prof_nes double, prof_payroll double, ruralurban_code int, ruralurban_category string);

insert into prof2016 select distinct a.county_code, a.pct_over_3, e.avg_downstream_speed, f.max_downstream_speed, b.payroll16, c.nes16, d.ruralurban_code, CASE
    WHEN d.ruralurban_code < 4 THEN 'Metro'
    WHEN d.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN d.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN d.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from above_three16 a left join prof_payroll b on a.county_code = b.stcnty join prof_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code join avg_speed16 e on d.county_code = e.county_code join max_speed16 f on e.county_code = f.county_code;

select avg(avg_speed), avg(pct_over_three), avg(prof_payroll), avg(prof_nes), ruralurban_code from prof2016 group by ruralurban_code order by ruralurban_code;

--looking at specific states
select avg(avg_speed), avg(pct_over_three), avg(prof_payroll), avg(prof_nes), ruralurban_code from prof2016 where strleft(stcnty, 2) = '46' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(pct_over_three), avg(prof_payroll), avg(prof_nes), ruralurban_code from prof2016 where strleft(stcnty, 2) = '39' group by ruralurban_code order by ruralurban_code;

--finding correlation between payroll and average speed for professional services
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select avg_speed as x, avg(avg_speed) over () as x_bar, prof_payroll as y, avg(prof_payroll) over () as y_bar from prof2016) ols;

--exploring professional services table, finding highest and lowest payrolls/NES
select avg(avg_speed), avg(pct_over_three), avg(prof_payroll), avg(prof_nes), ruralurban_code from prof2016 where strleft(stcnty, 2) = '36' group by ruralurban_code order by ruralurban_code;

select count(*) from fcc16 where max_cir_down = '1000' or max_cir_down = '0';

select * from prof2016 order by prof_payroll limit 30;

select * from prof2016 order by prof_payroll desc limit 30;

select * from prof2016 order by prof_nes limit 30;

select * from prof2016 order by prof_nes desc limit 30;

--recreating retail table with the variables of percentage of counties with over 3 mbps, 
--rural urban categories, payroll, NES, and the avg and max speeds
drop table if exists retail2016;
create table retail2016 (stcnty string, pct_over_three double, avg_speed double, max_speed double, nes double, payroll double, ruralurban_code int, ruralurban_category string);

insert into retail2016 select distinct a.county_code, a.pct_over_3, e.avg_downstream_speed, f.max_downstream_speed, b.payroll16, c.nes16, d.ruralurban_code, CASE
    WHEN d.ruralurban_code < 4 THEN 'Metro'
    WHEN d.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN d.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN d.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from above_three16 a left join retail_payroll b on a.county_code = b.stcnty join retail_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code join avg_speed16 e on d.county_code = e.county_code join max_speed16 f on e.county_code = f.county_code;

--exploring the retail tables for highest/lowest payroll and nes
select * from retail2016 order by nes desc limit 30;

select * from retail2016 order by nes limit 30;

select * from retail2016 order by payroll desc limit 30;

select * from retail2016 order by payroll limit 30;

--recreating agriculture table with the variables of percentage of counties with over 3 mbps, rural urban --categories, payroll, NES, and the avg and max speeds

drop table if exists agriculture2016;
create table agriculture2016 (stcnty string, pct_over_three double, avg_speed double, max_speed double, nes double, payroll double, ruralurban_code int, ruralurban_category string);

insert into agriculture2016 select distinct a.county_code, a.pct_over_3, e.avg_downstream_speed, f.max_downstream_speed, b.payroll16, c.nes16, d.ruralurban_code, CASE
    WHEN d.ruralurban_code < 4 THEN 'Metro'
    WHEN d.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN d.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN d.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from above_three16 a left join agriculture_payroll b on a.county_code = b.stcnty join agriculture_nes c on b.stcnty = c.stcnty join rucc d on c.stcnty = d.county_code join avg_speed16 e on d.county_code = e.county_code join max_speed16 f on e.county_code = f.county_code;


--introducing broadband speed test data for Seattle city to compare advertised average 
--broadband speeds with tested average broadband speeds for year 2016
drop table if exists seattle;
create external table seattle (id smallint, downspeed string, upspeed string, advertised_down string, advertised_up string, connection string, cost string, test_date string, isp string, isp_user string, min_rtt string, time_stamp string, census_block string)  row format delimited fields terminated by '\t' location '/user/sp4494/seattle/' tblproperties('skip.header.line.count'='1');

--calculating average broadband speeds at the census block GROUP level
drop table if exists group_avg16;
create external table group_avg16 (block_group string, avg_down double, avg_up double);

insert into group_avg16 select strleft(block_code, 12), avg(cast(max_ad_down as float)), avg(cast(max_ad_up as float)) from fcc16 group by strleft(block_code, 12);

--joining census block average speeds with seattle speed tests
select s.census_block, avg(f.avg_down) as max_ad_down, avg(f.avg_up) as max_ad_up, avg(cast(s.downspeed as float)) as seattle_down, avg(cast(s.upspeed as float)) as seattle_up from seattle s inner join group_avg16 f on f.block_group = s.census_block where regexp_like(test_date, '.*2016.*') is true and s.census_block is not null group by s.census_block;

select count(*), census_block from seattle where regexp_like(test_date, '.*2016.*') is true group by census_block;

--calculate percentage difference between advertised and tested broadband speeds for seattle area
select s.census_block, avg(f.avg_down) - avg(cast(s.downspeed as float)) as diff_ad_actual_down, avg(f.avg_up) - avg(cast(s.upspeed as float)) as diff_ad_actual_up from seattle s inner join group_avg16 f on f.block_group = s.census_block where regexp_like(test_date, '.*2016.*') is true and s.census_block is not null group by s.census_block having diff_ad_actual_down > 100 and diff_ad_actual_up > 100;

select avg(cast(max_ad_down as float)) avg_downstream_speed, avg(cast(max_ad_up as float)) avg_upstream_speed, county_code from fcc16 group by county_code having county_code = '53033';




--using consumer speeds rather than commercial speeds as it seems to depend on 
--whether there is a business or not giving many county blocks 0 values when there is a reasonable
--amount of consumer internet access

--using boolean column consumer to find percentage of county blocks in a county that have
--internet access or not
drop table if exists res_pct_internet16;
create external table res_pct_internet16 (county_code string, pct_with_internet double);

insert into res_pct_internet16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc16 where consumer = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;


--Avg Upstream and Downstream speeds per county with consumer speeds
drop table if exists res_avg_speed16;
create external table res_avg_speed16 (county_code string, avg_downstream_speed double, avg_upstream_speed double);

insert into res_avg_speed16 select county_code, avg(cast(max_ad_down as float)) avg_downstream_speed, avg(cast(max_ad_up as float)) avg_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;


--Select Max Downstream and Upstream with consumer speeds
drop table if exists res_max_speed16;
create external table res_max_speed16 (county_code string, max_downstream_speed double, max_upstream_speed double);

insert into res_max_speed16 select county_code, max(cast(max_ad_down as float)) max_downstream_speed, max(cast(max_ad_up as float)) max_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;



--Select Min Downstream and Upstream with Consumer Speeds
drop table if exists res_min_speed16;
create external table res_min_speed16 (county_code string, min_downstream_speed double, min_upstream_speed double);

insert into res_min_speed16 select county_code, min(cast(max_ad_down as float)) min_downstream_speed, min(cast(max_ad_up as float)) min_upstream_speed from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code;

--aggregating internet metrics at county level
drop table if exists county_internet_metrics;
create table county_internet_metrics (county_code string, avg_down double, max_down double, min_down double, pct_with_internet double);

insert into county_internet_metrics select distinct a.county_code, a.avg_downstream_speed, b.max_downstream_speed, c.min_downstream_speed, d.pct_with_internet from res_avg_speed16 a join res_max_speed16 b on a.county_code = b.county_code join res_min_speed16 c on b.county_code = c.county_code join res_pct_internet16 d on c.county_code = d.county_code;

--classify the agriculture industry 2016 counties by the rural-urban description
drop table if exists res_agriculture2016;
create table res_agriculture2016 (county_code string, avg_speed double, max_speed double, min_speed double,pct_with_internet double, payroll double, nes double, ruralurban_code int, ruralurban_category string);


insert into res_agriculture2016 select distinct b.county_code, b.avg_down, b.max_down, b.min_down, b.pct_with_internet, c.payroll16, d.nes16, e.ruralurban_code,  CASE
    WHEN e.ruralurban_code < 4 THEN 'Metro'
    WHEN e.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN e.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN e.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from county_internet_metrics b join agriculture_payroll c on b.county_code = c.stcnty join agriculture_nes d on c.stcnty = d.stcnty join rucc e on e.county_code = d.stcnty;

--grouping by the metropolitan category
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_category from res_agriculture2016 group by ruralurban_category;

--grouping by the RUCC 
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_agriculture2016 group by ruralurban_code order by ruralurban_code;

--order the counties by their highest and lowest average payroll for agriculture industry
select * from res_agriculture2016 order by payroll limit 30;

select * from res_agriculture2016 order by payroll desc limit 30;

--order the counties by their highest and lowest average NES receipts for agriculture industry
select * from res_agriculture2016 order by nes limit 30;

select * from res_agriculture2016 order by nes desc limit 30;

--creating retail table with finalized list of variables using consumer speeds
drop table if exists res_retail2016;
create table res_retail2016 (county_code string, avg_speed double, max_speed double, min_speed double,pct_with_internet double, payroll double, nes double, ruralurban_code int, ruralurban_category string);

insert into res_retail2016 select distinct b.county_code, b.avg_down, b.max_down, b.min_down, b.pct_with_internet, c.payroll16, d.nes16, e.ruralurban_code,  CASE
    WHEN e.ruralurban_code < 4 THEN 'Metro'
    WHEN e.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN e.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN e.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from county_internet_metrics b join retail_payroll c on b.county_code = c.stcnty join retail_nes d on c.stcnty = d.stcnty join rucc e on e.county_code = d.stcnty;

--grouping by the metropolitan category
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_category from res_retail2016 group by ruralurban_category;

--grouping by the RUCC 
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 group by ruralurban_code order by ruralurban_code;

--exploring highest/lowest NES and payroll
select * from res_retail2016 order by payroll limit 30;

select * from res_retail2016 order by payroll desc limit 30;

select * from res_retail2016 order by nes limit 30;

select * from res_retail2016 order by nes desc limit 30;

--exploring retail industry in specific states

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 where strleft(county_code, 2) = '46' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 where strleft(county_code, 2) = '47' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 where strleft(county_code, 2) = '39' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 where strleft(county_code, 2) = '36' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_retail2016 where strleft(county_code, 2) = '06' group by ruralurban_code order by ruralurban_code;

--correlation between avg speed and payroll broken down by state
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes order by state;

--correlation between avg speed and nes broken down by state
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, nes as y, avg(nes) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes order by state;

--correlation between avg speed and payroll broken down by state and rucc
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select max_speed  as x, avg(max_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes order by state;

--correlation between avg speed and nes broken down by state and rucc
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select min_speed  as x, avg(min_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes order by state;

--creating professional services specific table with finalized variables using consumer
drop table if exists res_prof2016;
create table res_prof2016 (county_code string, avg_speed double, max_speed double, min_speed double,pct_with_internet double, payroll double, nes double, ruralurban_code int, ruralurban_category string);


insert into res_prof2016 select distinct b.county_code, b.avg_down, b.max_down, b.min_down, b.pct_with_internet, c.payroll16, d.nes16, e.ruralurban_code,  CASE
    WHEN e.ruralurban_code < 4 THEN 'Metro'
    WHEN e.ruralurban_code = 4 THEN 'Adjacent to Metro' 
WHEN e.ruralurban_code = 6 THEN 'Adjacent to Metro'
WHEN e.ruralurban_code = 8 THEN 'Adjacent to Metro' 
ELSE 'Non-Metro' 
END AS ruralurban_continuum from county_internet_metrics b join prof_payroll c on b.county_code = c.stcnty join prof_nes d on c.stcnty = d.stcnty join rucc e on e.county_code = d.stcnty;

--grouping by metropolitan category
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_category from res_prof2016 group by ruralurban_category;

--grouping by rucc
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 group by ruralurban_code order by ruralurban_code;

--exploring highest/lowest payroll and nes
select * from res_prof2016 order by payroll limit 30;

select * from res_prof2016 order by payroll desc limit 30;

select * from res_prof2016 order by nes limit 30;

select * from res_prof2016 order by nes desc limit 30;

--exploring professional services industry in specific states

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '46' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '47' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '39' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '36' group by ruralurban_code order by ruralurban_code;

select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '06' group by ruralurban_code order by ruralurban_code;

--selecting pearson correlation between avg speed and payroll for professional services
--broken down by state and rucc
select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as x_bar, payroll as y, avg(payroll) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as y_bar, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from res_prof2016) ols) state_slopes order by state_fips, rucc;

--selecting pearson correlation between avg speed and NES for professional services
--broken down by state and rucc

select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as x_bar, nes as y, avg(nes) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as y_bar, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from res_prof2016) ols) state_slopes order by state_fips, rucc;
--selecting pearson correlation between avg speed and nes for professional services
--partitioned by state 
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select max_speed  as x, avg(max_speed) over (partition by strleft(county_code, 2)) as x_bar, nes as y, avg(nes) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_prof2016) ols) state_slopes order by state;


--experimenting with concat function
select concat(strleft(county_code,2), cast(ruralurban_code as string)) from res_prof2016 limit 5;

--looking at maryland broadband metrics
select * from county_internet_metrics where strleft(county_code, 2) = '24' order by county_code;


--calculating pearson correlation between average broadband speeds and average payroll for all the 
--states for 2016 displaying correlation higher than 0.9 partitioned by state FIPS code and RUCC
select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as x_bar, payroll as y, avg(payroll) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as y_bar, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from res_prof2016) ols) state_slopes where slope > .9 order by state_fips, rucc;

--calculating pearson correlation between average broadband speeds and average payroll for agriculture 
-- industry for 2016 displaying correlation higher than 0.9 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016) ols) state_slopes where slope > .9 order by state;

--calculating pearson correlation between average broadband speeds and average payroll for retail 
--industry for 2016 displaying negative correlation partitioned by state FIPS code and RUCC
select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as x_bar, payroll as y, avg(payroll) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as y_bar, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from res_retail2016) ols) state_slopes where slope< 0 order by rucc, state_fips;

--calculating pearson correlation between average broadband speeds and average payroll for retail
--industry for 2016 displaying correlation higher than 0.9 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes where slope > .9 order by state;

select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, count from (select count(*) as count, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from rucc group by concat(strleft(county_code,2), cast(ruralurban_code as string))) rucc order by state_fips, rucc;

--calculating pearson correlation between average broadband speeds less than 40 mbps and average 
--payroll for agriculture industry for 2016 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 40) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 45) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 35) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 25) ols) state_slopes order by state;

--calculating pearson correlation between average broadband speeds less than 5 mbps and average 
--payroll for retail industry for 2016
select sum((x - x_bar) * (y - y_bar)) / sqrt(sum((x - x_bar) * (x - x_bar)) * sum((y - y_bar) * (y - y_bar)))  as slope from (select avg_speed as x, avg(avg_speed) over () as x_bar, payroll as y, avg(payroll) over () as y_bar from res_prof2016 where avg_speed < 5) ols;


select * from res_prof2016 order by payroll desc limit 100;
select * from res_prof2016 order by payroll limit 100;
select * from res_prof2016 order by nes desc limit 100;
select * from res_prof2016 order by nes limit 100;


--displaying the table for state north dakota
select avg(avg_speed), avg(payroll), avg(nes), ruralurban_code from res_prof2016 where strleft(county_code, 2) = '38' group by ruralurban_code order by ruralurban_code;

--calculating pearson correlation between average broadband speeds and average payroll for each state 
--for the retail industry for 2016
select distinct strleft(state, 2) as state_fips, strright(state,1) as rucc, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as x_bar, nes as y, avg(nes) over (partition by concat(strleft(county_code,2), cast(ruralurban_code as string))) as y_bar, concat(strleft(county_code,2), cast(ruralurban_code as string)) as state from res_retail2016) ols) state_slopes order by rucc, state_fips;

--calculating pearson correlation between avg speed and nes for retail industry partitioned by state
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, nes as y, avg(nes) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016) ols) state_slopes order by state;


--finding the counties where the percentage of the county that had commercial internet speed 
--dropped by more than 30% in 2017
select distinct p.*, r.ruralurban_code, r.description from filtered_pct_all p join rucc r on r.county_code = p.county_code where p.pct16 - p.pct17 > .3;

select state, avg(cast(annual_payroll as int)) as payroll from cbp16 group by state order by payroll desc limit 20;

select b.county_code,  b.avg_downstream_speed, a.avg_payroll from res_avg_speed16 b join county_payroll a on a.county_code = b.county_code;

--creating table with average payroll for all industries at the county level
drop table if exists county_payroll;
create external table county_payroll (avg_payroll double, county_code string);

insert into county_payroll select avg(cast(annual_payroll as int)) as payroll, stcnty_code from cbp16 group by stcnty_code;

select b.county_code,  b.avg_downstream_speed, a.avg_payroll from res_avg_speed16 b join county_payroll a on a.county_code = b.county_code;

--calculating average downstream speeds and payrolls for overall industries in south dakota
--ordered by rucc
select avg(avg_down) as avg_downspeed, avg(avg_payroll) as avg_payroll, rucc as ruralurban_code from (select a.county_code as county, a.avg_downstream_speed as avg_down, b.avg_payroll as avg_payroll, c.ruralurban_code as rucc from res_avg_speed16 a join county_payroll b on a.county_code = strleft(b.county_code, 5) join rucc c on c.county_code =a.county_code where strleft(a.county_code, 2) = '46') sd group by ruralurban_code order by ruralurban_code;

--calculating average downstream speeds and payrolls for overall industries 
--industry in north dakota
--ordered by rucc
select avg(avg_down) as avg_downspeed, avg(avg_payroll) as avg_payroll, rucc as ruralurban_code from (select a.county_code as county, a.avg_downstream_speed as avg_down, b.avg_payroll as avg_payroll, c.ruralurban_code as rucc from res_avg_speed16 a join county_payroll b on a.county_code = strleft(b.county_code, 5) join rucc c on c.county_code =a.county_code where strleft(a.county_code, 2) = '38') sd group by ruralurban_code order by ruralurban_code;

--looking at overall payroll and avg downstream speeds for south dakota, ordered by rucc
select avg(avg_speed) as avg_downspeed, avg(payroll) as avg_payroll, ruralurban_code from res_prof2016 where strleft(county_code, 2) = '46' group by ruralurban_code order by ruralurban_code;

--looking at professional services industry payroll and avg downstream speeds for north dakota 
--ordered by rucc
select avg(avg_speed) as avg_downspeed, avg(payroll) as avg_payroll, ruralurban_code from res_prof2016 where strleft(county_code, 2) = '38' group by ruralurban_code order by ruralurban_code;

--looking at professional industry payroll and avg downstream speeds for california 
--ordered by rucc
select avg(avg_speed) as avg_downspeed, avg(payroll) as avg_payroll, avg(nes) as nes_receipts, ruralurban_code from res_prof2016 where strleft(county_code, 2) = '06' group by ruralurban_code order by ruralurban_code;
--overall industry for california ordered by rucc
select avg(avg_down) as avg_downspeed, avg(avg_payroll) as avg_payroll, rucc as ruralurban_code from (select a.county_code as county, a.avg_downstream_speed as avg_down, b.avg_payroll as avg_payroll, c.ruralurban_code as rucc from res_avg_speed16 a join county_payroll b on a.county_code = strleft(b.county_code, 5) join rucc c on c.county_code =a.county_code where strleft(a.county_code, 2) = '38') sd group by ruralurban_code order by ruralurban_code;

--looking at percentage of counties with internet when measuring by speed rather than the reported boolean value
--looked at whether commercial internet speeds were greater than 0 as
--many census blocks had a 1 in the business column indicating access but had 0s in the speeds. 

--2017 table percentage of census blocks in a county with speeds > 0
drop table if exists zero_pct_internet17;
create external table zero_pct_internet17 (county_code string, pct_with_internet double);

insert into zero_pct_internet17 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc17 where cast(max_cir_down as float) > 0 and cast(max_cir_up as float) > 0 and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select * from zero_pct_internet17 limit 10;

--2016 table percentage of census blocks in a county with speeds > 0

drop table if exists zero_pct_internet16;
create external table zero_pct_internet16 (county_code string, pct_with_internet double);

insert into zero_pct_internet16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc16 where cast(max_cir_down as float) > 0 and cast(max_cir_up as float) > 0 and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select * from zero_pct_internet16 limit 10;

--2015 table percentage of census blocks in a county with speeds > 0

drop table if exists zero_pct_internet15;

create external table zero_pct_internet15 (county_code string, pct_with_internet double);

insert into zero_pct_internet15 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc15 where cast(max_cir_down as float) > 0 and cast(max_cir_up as float) > 0 and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

select * from zero_pct_internet15 limit 10;

drop table if exists zero_pct_with_internet_all;

--Combining 2015,2016,2017 table percentage of census blocks in a county with speeds > 0


create external table zero_pct_with_internet_all (county_code string, pct2015 double, pct2016 double, pct2017 double);

insert into zero_pct_with_internet_all select distinct a.county_code, a.pct_with_internet, b.pct_with_internet, c.pct_with_internet from zero_pct_internet15 a inner join zero_pct_internet16 b on a.county_code = b.county_code inner join zero_pct_internet17 c on c.county_code = b.county_code;

--Finding where drop in 2017 > .3
select * from zero_pct_with_internet_all where pct2016 - pct2017 > .3;

select z.*, r.ruralurban_code from zero_pct_with_internet_all z join rucc r on z.county_code = r.county_code where pct2016 - pct2017 > .3;

--for all counties whose internet access dropped by more than 30% calculates counts grouped by rucc
select count(z.county_code) as num_dropped_by_30, r.ruralurban_code as rucc from zero_pct_with_internet_all z join rucc r on z.county_code = r.county_code where z.pct2016 - z.pct2017 > .3 group by rucc order by rucc;

--showing difference for 2017 between internet access defined by fcc vs by speeds
select a.county_code, a.pct2017 as pct_marked_true_fcc, b.pct2017 as pct_greaterthan0 from pct_with_internet_all a join zero_pct_with_internet_all b on a.county_code = b.county_code limit 30;


