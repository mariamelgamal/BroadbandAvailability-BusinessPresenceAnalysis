
--create cbp 2015 table 
drop table if exists cbp15;
create external table cbp15 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business15/';

--create cbp 2016 table 
drop table if exists cbp16;
create external table cbp16 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business16/';

drop table if exists cbp17;
create external table cbp17 (stcnty_code string, state string, county string, naics string, industry string, naics_dsc string, employment string, employment_flag string, annual_payroll string, annual_payroll_flag string, nes_receipts string, nes_receipts_flag string) row format delimited fields terminated by ',' location '/user/sp4494/business/';

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


-- Percentage of blocks in each county with internet using fcc boolean definition

drop table if exists pct_internet17;
create external table pct_internet17 (county_code string, pct_with_internet double);

insert into pct_internet17 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc17 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc17 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

drop table if exists pct_internet16;
create external table pct_internet16 (county_code string, pct_with_internet double);

insert into pct_internet16 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc16 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc16 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;

drop table if exists pct_internet15;
create external table pct_internet15 (county_code string, pct_with_internet double);

insert into pct_internet15 select total.county_code as county,  round(num_with_internet / num_total, 4) as percentage from (select count(distinct block_code) num_total, county_code from fcc15 where state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI' group by county_code) total inner join (select count(distinct block_code) num_with_internet, county_code from fcc15 where business = '1' and state != 'MP' and state != 'GU' and state != 'AS' and state != 'PR' and state != 'VI'  group by county_code) with_internet on total.county_code = with_internet.county_code;


--average annual payroll per industry per county
--average annual payroll per industry per county for year 2017

drop table if exists cbp17_averagepayroll;
create external table cbp17_averagepayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp17_averagepayroll select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp17 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;


--average annual payroll per industry per county for year 2016
drop table if exists cbp16_avgpayroll; 
create external table cbp16_avgpayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp16_avgpayroll select (CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2))) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp16 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;


--average annual payroll per industry per county for year 2015
drop table if exists cbp15_avgpayroll;
create external table cbp15_avgpayroll (stcnty_naics string, avg_payroll_per_industry_per_county double);

insert into cbp15_avgpayroll select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(annual_payroll as float)),2) as avg_payroll from cbp15 where annual_payroll != "N" and annual_payroll != "D" group by industry_per_county;



--combine average payroll over the years
drop table if exists all_avgpayroll;
create external table all_avgpayroll(stcnty_naics string, avg_payroll2015 double, avg_payroll2016 double, avg_payroll2017 double);

insert into all_avgpayroll select distinct a.stcnty_naics, a.avg_payroll_per_industry_per_county, b.avg_payroll_per_industry_per_county, c.avg_payroll_per_industry_per_county from cbp15_avgpayroll c inner join cbp16_avgpayroll b on c.stcnty_naics = b.stcnty_naics inner join cbp17_averagepayroll a on a.stcnty_naics = b.stcnty_naics;


-- average NES receipts per industry per county
-- average NES receipts per industry per county for year 2017
drop table if exists cbp17_avgnesreceipts;
create external table cbp17_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp17_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp17 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;


-- average NES receipts per industry per county for year 2016
drop table if exists cbp16_avgnesreceipts;

create external table cbp16_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp16_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp16 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;


-- average NES receipts per industry per county for year 2015
drop table if exists cbp15_avgnesreceipts;

create external table cbp15_avgnesreceipts (stcnty_naics string, avg_nes_per_industry_per_county double);

insert into cbp15_avgnesreceipts select CONCAT(lpad(stcnty_code,5,"0"), "-", left(trim(naics),2)) as industry_per_county, round(avg(cast(nes_receipts as float)),2) as avg_nesreceipts from cbp15 where trim(nes_receipts) != "N" and trim(nes_receipts) != "D" group by industry_per_county;


--Combine Average NES Receipts over the years (2015, 2016, 2017)
drop table if exists all_avgnesreceipts;

create external table all_avgnesreceipts(stcnty_naics string, avg_nesreceipts2015 double, avg_nesreceipts2016 double, avg_nesreceipts2017 double);

insert into all_avgnesreceipts select distinct a.stcnty_naics, a.avg_nes_per_industry_per_county, b.avg_nes_per_industry_per_county, c.avg_nes_per_industry_per_county from cbp15_avgnesreceipts c inner join cbp16_avgnesreceipts b on c.stcnty_naics = b.stcnty_naics inner join cbp17_avgnesreceipts a on a.stcnty_naics = b.stcnty_naics;




-- Create Agriculture table
drop table if exists agriculture_payroll;
create table agriculture_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into agriculture_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '11' and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;


-- Create Retail table
drop table if exists retail_payroll;
create external table retail_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into retail_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '44' and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;




-- Create Professional table
drop table if exists prof_payroll;
create table prof_payroll (stcnty string, stcnty_naics string, payroll15 double, payroll16 double, payroll17 double);

insert into prof_payroll select strleft(stcnty_naics, 5), stcnty_naics, avg_payroll2015, avg_payroll2016, avg_payroll2017 from all_avgpayroll where strright(stcnty_naics, 2) = '54'  and avg_payroll2015 is not null and avg_payroll2016 is not null and avg_payroll2017 is not null;



--Creating rural urban continuum code table
drop table if exists rucc;
create external table rucc (county_code string, state string, county string, population string, ruralurban_code smallint, description string)  row format delimited fields terminated by '\t' location '/user/sp4494/ruralurban/' tblproperties('skip.header.line.count'='1');




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


--creating table with average payroll for all industries at the county level
drop table if exists county_payroll;
create external table county_payroll (avg_payroll double, county_code string);

insert into county_payroll select avg(cast(annual_payroll as int)) as payroll, stcnty_code from cbp16 group by stcnty_code;


--Exploring threshold for Agriculture industries
--calculating pearson correlation between average broadband speeds less than 40 mbps and average 
--payroll for agriculture industry for 2016 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 40) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 45) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 35) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_agriculture2016 where avg_speed < 25) ols) state_slopes order by state;


--Exploring threshold for Retail industries
--calculating pearson correlation between average broadband speeds less than 40 mbps and average 
--payroll for retail industry for 2016 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016 where avg_speed < 40) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016 where avg_speed < 45) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016 where avg_speed < 35) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_retail2016 where avg_speed < 25) ols) state_slopes order by state;


--Exploring threshold for Professional Services industries
--calculating pearson correlation between average broadband speeds less than 40 mbps and average 
--payroll for prof services industry for 2016 partitioned by state FIPS code and RUCC
select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_prof2016 where avg_speed < 40) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_prof2016 where avg_speed < 45) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_prof2016 where avg_speed < 35) ols) state_slopes order by state;

select distinct state, slope from (select state, sum((x - x_bar) * (y - y_bar)) over(partition by state)/ sqrt(sum((x - x_bar) * (x - x_bar)) over(partition by state) * sum((y - y_bar) * (y - y_bar)) over(partition by state))  as slope from (select avg_speed  as x, avg(avg_speed) over (partition by strleft(county_code, 2)) as x_bar, payroll as y, avg(payroll) over (partition by strleft(county_code, 2)) as y_bar, strleft(county_code, 2) as state from res_prof2016 where avg_speed < 25) ols) state_slopes order by state;





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


