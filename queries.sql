-- query-A - Total number of devices & active devices week wise
SELECT week_number AS "WEEK Number", count(distinct(imei_tac)) as "Number of devices", count(case when system_status = 'ACTIVE' THEN 1 end) as "Active Devices" from master_data_table
where imei_tac != 'NA' and system_status != 'NA'
group by week_number
order by week_number;


-- query-B - Total number of customers & active customers week wise
CREATE OR REPLACE  VIEW WEEKLY_TOTAL_ACTIVE_CUSTOMERS AS
SELECT week_number AS "WEEK Number", COUNT(DISTINCT(msisdn)) AS "Total Number of customers", COUNT(CASE WHEN system_status = 'ACTIVE' THEN 1 END) AS "Active customers" FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE system_status != 'NA'
GROUP BY week_number
ORDER BY week_number;
select * from TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE;
SELECt * FROM WEEKLY_TOTAL_ACTIVE_CUSTOMERS;

-- query-C - Total Revenue by Male vs Female customers week wise
TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE;

CREATE OR REPLACE  VIEW WEEKLY_MALE_FEMALE_REVENUE AS
SELECT week_number AS "WEEK NUMBER",
       gender,
       SUM(revenue_usd) AS "TOTAL REVENUE"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND gender IN ('MALE', 'FEMALE')
GROUP BY week_number, gender
ORDER BY week_number;

select * from WEEKLY_MALE_FEMALE_REVENUE;

-- query-D - Total Revenue distribution by age of the customer week wise
CREATE OR REPLACE VIEW WEEKLY_REVENUE_AGE_CUSTOMER AS
SELECT week_number AS "WEEK NUMBER",
       CAST((YEAR(CURRENT_DATE()) - year_of_birth) AS INTEGER) AS age,
       SUM(revenue_usd) AS "TOTAL REVENUE"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND year_of_birth != 'NA' AND year_of_birth != 0
GROUP BY week_number, year_of_birth
ORDER BY week_number, age;

SELECT * FROM WEEKLY_REVENUE_AGE_CUSTOMER;

-- query-E - Total Revenue distribution by the value_segment
CREATE OR REPLACE  VIEW WEEKLY_REVENUE_VALUE_SEGMENT  AS
SELECT week_number as "WEEK NUMBER", value_segment AS "VALUE SEGMENT", CAST((YEAR(CURRENT_DATE()) - year_of_birth) AS INTEGER) AS age,SUM(revenue_usd) AS "TOTAL REVENUE"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM MASTER_DATA_TABLE) OR week_number = 35)
  AND value_segment != 'NA' AND year_of_birth != 0
GROUP BY week_number, age ,value_segment
ORDER BY week_number, age;
SELECT * FROM WEEKLY_REVENUE_VALUE_SEGMENT;

-- QUERY - F - TOTAL Revenue distribution by mobile_type 
CREATE OR REPLACE  VIEW WEEKLY_REVENUE_MOBILE_TYPE  AS
SELECT week_number AS "Week Number", mobile_type AS "Mobile Type", SUM(revenue_usd) AS "Total Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND mobile_type != 'NA'
GROUP BY week_number, mobile_type
ORDER BY week_number, mobile_type;
SELECT * FROM WEEKLY_REVENUE_MOBILE_TYPE;
-- query - G - Total Revenue distribution by brand_name
CREATE OR REPLACE  VIEW WEEKLY_REVENUE_BRAND_NAME AS
SELECT week_number AS "Week Number", brand_name AS "Brand Name", SUM(revenue_usd) AS "Total Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND brand_name != 'NA'
GROUP BY week_number, brand_name
ORDER BY week_number, brand_name;
SELECT * FROM WEEKLY_REVENUE_BRAND_NAME;
-- query - H - Total Revenue distribution by os_name 
CREATE OR REPLACE  VIEW WEEKLY_REVENUE_OS_NAME AS
SELECT week_number AS "Week Number", os_name AS "OS Name", SUM(revenue_usd) AS "Total Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND os_name != 'NA'
GROUP BY week_number, os_name
ORDER BY week_number, os_name;
SELECT * FROM WEEKLY_REVENUE_OS_NAME;
-- query - i - Total Revenue distribution by os_vendor
CREATE OR REPLACE VIEW WEEKLY_REVENUE_OS_VENDOR AS
SELECT week_number AS "Week Number", os_vendor AS "OS Vendor", ROUND(SUM(revenue_usd), 2) AS "Total Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND os_vendor != 'NA'
GROUP BY week_number, os_vendor
ORDER BY week_number, os_vendor;
SELECT * FROM WEEKLY_REVENUE_OS_VENDOR;

-- query - j -Total distribution of customer by os_name
CREATE OR REPLACE  VIEW WEEKLY_DISTRIBUTION_OS_NAME AS
SELECT week_number AS "Week Number", os_name AS "OS Name", COUNT(*) AS "Total Distribution"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND os_name != 'NA'
GROUP BY week_number, os_name
ORDER BY week_number, os_name;
SELECT * FROM WEEKLY_DISTRIBUTION_OS_NAME;
-- query k - Total distribution of customer by brand_name 
CREATE OR REPLACE VIEW WEEKLY_DISTRIBUTION_BRAND_NAME AS
SELECT week_number AS "Week Number", brand_name AS "Brand Name", COUNT(*) AS "Total Distribution"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND brand_name != 'NA'
GROUP BY week_number, brand_name
ORDER BY week_number, brand_name;
SELECT * FROM WEEKLY_DISTRIBUTION_BRAND_NAME;

-- query L - Total distribution of customer by mobile_type
CREATE OR REPLACE  VIEW WEEKLY_DISTRIBUTION_MOBILE_TYPE AS
SELECT week_number AS "Week Number", mobile_type AS "Mobile Type", COUNT(*) AS "Total Distribution"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND mobile_type != 'NA'
GROUP BY week_number, mobile_type
ORDER BY week_number, mobile_type;
SELECT * FROM WEEKLY_DISTRIBUTION_MOBILE_TYPE;

-- query m - Week wise highest and lowest revenue by brand_name
CREATE OR REPLACE  VIEW WEEKLY_HIGHEST_LOWEST_BRAND_NAME AS
SELECT week_number AS "Week Number", brand_name AS "Brand Name", 
       ROUND(MAX(revenue_usd), 2) AS "Highest Revenue", 
       ROUND(MIN(revenue_usd), 2) AS "Lowest Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND brand_name != 'NA'
GROUP BY week_number, brand_name
ORDER BY week_number, brand_name;
select * from WEEKLY_HIGHEST_LOWEST_BRAND_NAME;
-- query n = Week wise highest and lowest revenue by os_name
CREATE OR REPLACE VIEW WEEKLY_HIGHEST_LOWEST_OS_NAME AS
SELECT week_number AS "Week Number", os_name AS "OS Name", 
       ROUND(MAX(revenue_usd), 2) AS "Highest Revenue", 
       ROUND(MIN(revenue_usd), 2) AS "Lowest Revenue"
FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE
WHERE (week_number < (SELECT MAX(week_number) FROM TELECOM_CUSTOMER_REVENUE_DB.TELECOM_CUSTOMER_REVENUE_SCHEMA.MASTER_DATA_TABLE) OR week_number = 35)
  AND os_name != 'NA'
GROUP BY week_number, os_name
ORDER BY week_number, os_name;
select * from WEEKLY_HIGHEST_LOWEST_OS_NAME;






--------------------------------------------------------------------------
-- Overall queries
--------------------------------------------------------------------------

-- query a- Total number of devices & active devices overall
-- query b - Total number of customers & active customers 
CREATE OR REPLACE  VIEW OVERALL_TOTAL_ACTIVE_CUSTOMERS AS
SELECT  COUNT(DISTINCT(msisdn)) AS "Total Number of Customers", COUNT(DISTINCT CASE WHEN system_status = 'ACTIVE' THEN msisdn END) AS "Active customers" FROM MASTER_DATA_TABLE
WHERE system_status != 'NA';

SELECT * FROM OVERALL_TOTAL_ACTIVE_CUSTOMERS;
-- query c - Total Revenue by Male vs Female customers
CREATE OR REPLACE VIEW OVERALL_MALE_FEMALE_REVENUE AS
SELECT gender AS "Gender", SUM(revenue_usd) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE (gender = 'MALE' OR gender = 'FEMALE')
GROUP BY gender;

SELECT * FROM OVERALL_MALE_FEMALE_REVENUE;
-- query d - Total Revenue distribution by age of the customer 
CREATE OR REPLACE VIEW OVERALL_REVENUE_AGE_CUSTOMER AS
SELECT CAST((YEAR(CURRENT_DATE()) - year_of_birth) AS INTEGER) AS Age, SUM(revenue_usd) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE year_of_birth != 'NA'
GROUP BY year_of_birth
ORDER BY Age;
SELECT * FROM OVERALL_REVENUE_AGE_CUSTOMER;

-- query e - Total Revenue distribution by the value_segment
CREATE OR REPLACE VIEW OVERALL_REVENUE_VALUE_SEGMENT AS
SELECT  value_segment AS "Value Segment", SUM(revenue_usd) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE value_segment != 'NA'
GROUP BY value_segment;
SELECT * FROM OVERALL_REVENUE_VALUE_SEGMENT;
-- query f - Total Revenue distribution by mobile_type
CREATE OR REPLACE VIEW OVERALL_REVENUE_MOBILE_TYPE AS
SELECT mobile_type AS "Mobile Type", SUM(revenue_usd) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE mobile_type != 'NA'
GROUP BY mobile_type
ORDER BY mobile_type;
SELECT * FROM OVERALL_REVENUE_MOBILE_TYPE;
-- query g - Total Revenue distribution by brand_name
CREATE OR REPLACE VIEW OVERALL_REVENUE_BRAND_NAME AS
SELECT brand_name AS "Brand Name", ROUND(SUM(revenue_usd), 2) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE brand_name != 'NA'
GROUP BY brand_name
ORDER BY brand_name;
SELECT * FROM OVERALL_REVENUE_BRAND_NAME;
-- query h - Total Revenue distribution by os_name
CREATE OR REPLACE VIEW OVERALL_REVENUE_OS_NAME AS
SELECT os_name AS "OS Name", ROUND(SUM(revenue_usd), 2) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE os_name != 'NA'
GROUP BY os_name
ORDER BY os_name;
SELECT * FROM OVERALL_REVENUE_OS_NAME;
-- query i - Total Revenue distribution by os_vendor
CREATE OR REPLACE VIEW OVERALL_REVENUE_OS_VENDOR AS
SELECT os_vendor AS "OS Vendor", SUM(revenue_usd) AS "Total Revenue"
FROM MASTER_DATA_TABLE
WHERE os_vendor != 'NA'
GROUP BY os_vendor
ORDER BY os_vendor;
SELECT * FROM OVERALL_REVENUE_OS_VENDOR;
-- query j - Total distribution of customer by os_name
CREATE OR REPLACE VIEW OVERALL_DISTRIBUTION_OS_NAME AS
SELECT os_name AS "OS Name", COUNT(DISTINCT msisdn) AS "Total Distribution"
FROM MASTER_DATA_TABLE
WHERE os_name != 'NA'
GROUP BY os_name
ORDER BY os_name;
SELECT * FROM OVERALL_DISTRIBUTION_OS_NAME;

-- query k - Total distribution of customer by brand_name
CREATE OR REPLACE VIEW OVERALL_DISTRIBUTION_BRAND_NAME AS
SELECT brand_name AS "Brand Name", COUNT(DISTINCT msisdn) AS "Total Distribution"
FROM MASTER_DATA_TABLE
WHERE brand_name != 'NA'
GROUP BY brand_name
ORDER BY brand_name;
SELECT * FROM OVERALL_DISTRIBUTION_BRAND_NAME;

-- query L - Total distribution of customer by mobile_type
CREATE OR REPLACE VIEW OVERALL_DISTRIBUTION_MOBILE_TYPE AS
SELECT mobile_type AS "Mobile Type", COUNT(DISTINCT msisdn) AS "Total Distribution"
FROM MASTER_DATA_TABLE
WHERE mobile_type != 'NA'
GROUP BY mobile_type
ORDER BY mobile_type;
SELECT * FROM OVERALL_DISTRIBUTION_MOBILE_TYPE;

-- query o - Distribution of brand_name by age : age segment can be of 10/20 year range
CREATE OR REPLACE VIEW DISTRIBUTION_BRAND_NAME_AGE_SEGMENT AS
SELECT brand_name as "Brand Name",
    CASE
        WHEN (year_of_birth = 2023 OR year_of_birth = 0) THEN '0 - 10'
        ELSE CONCAT(FLOOR((YEAR(CURRENT_DATE()) - year_of_birth - 1) / 10) * 10, '-', CEIL((YEAR(CURRENT_DATE()) - year_of_birth) / 10) * 10)
    END AS "Age Segment",
    COUNT(*) AS "Distribution"
FROM MASTER_DATA_TABLE
WHERE year_of_birth != 'NA' AND brand_name != 'NA'
GROUP BY brand_name, "Age Segment"
ORDER BY "Age Segment",brand_name;
SELECT * FROM DISTRIBUTION_BRAND_NAME_AGE_SEGMENT;

-- query p - Distribution of os_name by age : age segment can be of 10/20 year range
CREATE OR REPLACE VIEW DISTRIBUTION_OS_NAME_AGE_SEGMENT AS
SELECT os_name,
    CASE
        WHEN (year_of_birth = 2023 OR year_of_birth = 0) THEN '0 - 10'
        ELSE CONCAT(FLOOR((YEAR(CURRENT_DATE()) - year_of_birth - 1) / 10) * 10, '-', CEIL((YEAR(CURRENT_DATE()) - year_of_birth) / 10) * 10)
    END AS "Age Segment",
    COUNT(*) AS "Distribution"
FROM MASTER_DATA_TABLE
WHERE year_of_birth != 'NA' AND os_name != 'NA'
GROUP BY os_name, "Age Segment"
ORDER BY "Age Segment",os_name;
SELECT * FROM DISTRIBUTION_OS_NAME_AGE_SEGMENT;

SELECT COUNT(*) FROM MASTER_DATA_TABLE;
SELECT COUNT(*) FROM MASTER_TABLE;
