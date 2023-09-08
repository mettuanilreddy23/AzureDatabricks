# Databricks notebook source
# MAGIC %md
# MAGIC ##### SPARK SQL DATA TYPES.
# MAGIC * The following table shows the type names as well as aliases used in Spark SQL parser for each data type.
# MAGIC
# MAGIC #### Pyspark-Datatype	=> SQL Data Type
# MAGIC * BooleanType	__`BOOLEAN`__
# MAGIC * ByteType	__`BYTE`, `TINYINT`__
# MAGIC * ShortType	__`SHORT`, `SMALLINT`__
# MAGIC * IntegerType	__`INT`, `INTEGER`__
# MAGIC * LongType	__`LONG`, `BIGINT`__
# MAGIC * FloatType	__`FLOAT`, `REAL`__
# MAGIC * DoubleType	__`DOUBLE`__
# MAGIC * DateType	__`DATE`__
# MAGIC * TimestampType	__`TIMESTAMP`__
# MAGIC * StringType	__`STRING`__
# MAGIC * BinaryType	__`BINARY`__
# MAGIC * DecimalType	__`DECIMAL`, `DEC`, `NUMERIC`__
# MAGIC * CalendarIntervalType	__`INTERVAL`__
# MAGIC * ArrayType	__`ARRAY<element_type>`__
# MAGIC * StructType	__`STRUCT<field1_name: field1_type, field2_name: field2_type, …>`__
# MAGIC * MapType	__`MAP<key_type, value_type>`__

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating databases for each stage
# MAGIC * staging zone / Bronze - `stg_sales`
# MAGIC * curation zone / silver - `curation_sales`
# MAGIC * data warehouse zone  / gold - `dw_sales`

# COMMAND ----------

#%fs rm -r /user/hive/warehouse/

# COMMAND ----------

# MAGIC %sql
# MAGIC /****************************************************
# MAGIC   DROPPING DATABASES USING CASDADE IF Already Exists
# MAGIC ****************************************************/
# MAGIC DROP DATABASE IF  EXISTS stg_sales CASCADE;
# MAGIC DROP DATABASE IF  EXISTS curation_sales CASCADE;
# MAGIC DROP DATABASE IF  EXISTS dw_sales CASCADE;
# MAGIC DROP DATABASE IF  EXISTS log_db CASCADE;
# MAGIC DROP DATABASE IF  EXISTS JOBS CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC /***************************************************************
# MAGIC   CREATING DATABASES for staging , curation and target warehouse
# MAGIC ****************************************************************/
# MAGIC CREATE DATABASE IF NOT EXISTS stg_sales  COMMENT 'This database we are using for to store staging data. it will be daily truncate and load.';
# MAGIC CREATE DATABASE IF NOT EXISTS curation_sales  COMMENT 'This database we are using for to store validated data. Data load will be  incremental load.';
# MAGIC CREATE DATABASE IF NOT EXISTS dw_sales  COMMENT 'This database we are using for to store aggregated data. Data load will be incremental and dimension and fact tables will be there';
# MAGIC CREATE DATABASE IF NOT EXISTS log_db COMMENT 'This database we are using for to store error log and audit logs';
# MAGIC CREATE DATABASE IF  NOT EXISTS JOBS COMMENT 'This database we are using for to store job table metadata information';

# COMMAND ----------

# MAGIC %sql
# MAGIC /***************************************************************
# MAGIC   Log table and error table creation.
# MAGIC ****************************************************************/
# MAGIC DROP TABLE IF EXISTS `log_db`.`job_audit_log`;
# MAGIC CREATE TABLE IF NOT EXISTS `log_db`.`job_audit_log` ( `created_date` TIMESTAMP, `log_type` VARCHAR(200), `log_description` VARCHAR(2000)) USING delta;
# MAGIC DROP TABLE IF EXISTS `log_db`.`job_bad_data_log`;
# MAGIC CREATE TABLE IF NOT EXISTS `log_db`.`job_bad_data_log` ( `_corrupt_record` STRING, `input_file_name` STRING, `UPDATED_DATE` TIMESTAMP) USING delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /***************************************************************
# MAGIC  CREATE TABLE DDL Scirpts for Staging tables
# MAGIC ****************************************************************/
# MAGIC USE  stg_sales;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `stg_sales`.`stg_channels`;
# MAGIC CREATE TABLE IF NOT EXISTS `stg_sales`.`stg_channels` ( `CHANNEL_ID` INT, `CHANNEL_DESC` STRING, `CHANNEL_CLASS` STRING, `CHANNEL_CLASS_ID` INT, `CHANNEL_TOTAL` STRING, `CHANNEL_TOTAL_ID` INT,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING ) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `stg_sales`.`stg_sales_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `stg_sales`.`stg_sales_transaction` ( `PROD_ID` INT, `CUST_ID` INT, `TIME_ID` STRING, `CHANNEL_ID` INT, `PROMO_ID` INT, `QUANTITY_SOLD` DOUBLE, `AMOUNT_SOLD` DOUBLE,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `stg_sales`.`stg_costs_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `stg_sales`.`stg_costs_transaction` ( `PROD_ID` INT, `TIME_ID` STRING, `PROMO_ID` INT, `CHANNEL_ID` INT, `UNIT_COST` DOUBLE, `UNIT_PRICE` DOUBLE, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `stg_sales`.`stg_product`;
# MAGIC CREATE TABLE IF NOT EXISTS  `stg_sales`.`stg_product` ( `PROD_ID` INT, `PROD_NAME` STRING, `PROD_DESC` STRING, `PROD_SUBCATEGORY` STRING, `PROD_SUBCATEGORY_ID` STRING, `PROD_SUBCATEGORY_DESC` STRING, `PROD_CATEGORY` STRING, `PROD_CATEGORY_ID` STRING, `PROD_CATEGORY_DESC` STRING, `PROD_WEIGHT_CLASS` STRING, `PROD_UNIT_OF_MEASURE` STRING, `PROD_PACK_SIZE` STRING, `SUPPLIER_ID` STRING, `PROD_STATUS` STRING, `PROD_LIST_PRICE` STRING, `PROD_MIN_PRICE` STRING, `PROD_TOTAL` STRING, `PROD_TOTAL_ID` STRING, `PROD_SRC_ID` STRING, `PROD_EFF_FROM` STRING, `PROD_EFF_TO` STRING, `PROD_VALID` STRING, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC DROP TABLE IF  EXISTS  `stg_sales`.`stg_countries`;
# MAGIC CREATE TABLE IF NOT EXISTS  `stg_sales`.`stg_countries` ( `COUNTRY_ID` INT, `COUNTRY_ISO_CODE` STRING, `COUNTRY_NAME` STRING, `COUNTRY_SUBREGION` STRING, `COUNTRY_SUBREGION_ID` INT, `COUNTRY_REGION` STRING, `COUNTRY_REGION_ID` INT, `COUNTRY_TOTAL` STRING, `COUNTRY_TOTAL_ID` INT, `COUNTRY_NAME_HIST` STRING, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC DROP TABLE IF  EXISTS  `stg_sales`.`stg_customers`;
# MAGIC CREATE TABLE IF NOT EXISTS  `stg_sales`.`stg_customers` ( `CUST_ID` INT, `CUST_FIRST_NAME` STRING, `CUST_LAST_NAME` STRING, `CUST_GENDER` STRING, `CUST_YEAR_OF_BIRTH` INT, `CUST_MARITAL_STATUS` STRING, `CUST_STREET_ADDRESS` STRING, `CUST_POSTAL_CODE` INT, `CUST_CITY` STRING, `CUST_CITY_ID` STRING, `CUST_STATE_PROVINCE` STRING, `CUST_STATE_PROVINCE_ID` STRING, `COUNTRY_ID` INT, `CUST_MAIN_PHONE_NUMBER` STRING, `CUST_INCOME_LEVEL` STRING, `CUST_CREDIT_LIMIT` STRING, `CUST_EMAIL` STRING, `CUST_TOTAL` STRING, `CUST_TOTAL_ID` STRING, `CUST_SRC_ID` STRING, `CUST_EFF_FROM` STRING, `CUST_EFF_TO` STRING, `CUST_VALID` STRING, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `stg_sales`.`stg_promotions`;
# MAGIC CREATE TABLE IF NOT EXISTS `stg_sales`.`stg_promotions` ( `PROMO_ID` INT, `PROMO_NAME` STRING, `PROMO_SUBCATEGORY` STRING, `PROMO_SUBCATEGORY_ID` INT, `PROMO_CATEGORY` STRING, `PROMO_CATEGORY_ID` INT,
# MAGIC `PROMO_COST` INT,
# MAGIC `PROMO_BEGIN_DATE` STRING,
# MAGIC `PROMO_END_DATE` STRING,
# MAGIC `PROMO_TOTAL` STRING,
# MAGIC `PROMO_TOTAL_ID` INT, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING) USING delta;
# MAGIC DROP TABLE IF  EXISTS  `stg_sales`.`stg_times`;
# MAGIC CREATE TABLE IF NOT EXISTS  `stg_sales`.`stg_times` (
# MAGIC   `TIME_ID` STRING,
# MAGIC   `DAY_NAME` STRING,
# MAGIC   `DAY_NUMBER_IN_WEEK` INT,
# MAGIC   `DAY_NUMBER_IN_MONTH` INT,
# MAGIC   `CALENDAR_WEEK_NUMBER` INT,
# MAGIC   `FISCAL_WEEK_NUMBER` INT,
# MAGIC   `WEEK_ENDING_DAY` STRING,
# MAGIC   `WEEK_ENDING_DAY_ID` INT,
# MAGIC   `CALENDAR_MONTH_NUMBER` INT,
# MAGIC   `FISCAL_MONTH_NUMBER` INT,
# MAGIC   `CALENDAR_MONTH_DESC` STRING,
# MAGIC   `CALENDAR_MONTH_ID` INT,
# MAGIC   `FISCAL_MONTH_DESC` STRING,
# MAGIC   `FISCAL_MONTH_ID` INT,
# MAGIC   `DAYS_IN_CAL_MONTH` INT,
# MAGIC   `DAYS_IN_FIS_MONTH` INT,
# MAGIC   `END_OF_CAL_MONTH` STRING,
# MAGIC   `END_OF_FIS_MONTH` STRING,
# MAGIC   `CALENDAR_MONTH_NAME` STRING,
# MAGIC   `FISCAL_MONTH_NAME` STRING,
# MAGIC   `CALENDAR_QUARTER_DESC` STRING,
# MAGIC   `CALENDAR_QUARTER_ID` INT,
# MAGIC   `FISCAL_QUARTER_DESC` STRING,
# MAGIC   `FISCAL_QUARTER_ID` INT,
# MAGIC   `DAYS_IN_CAL_QUARTER` INT,
# MAGIC   `DAYS_IN_FIS_QUARTER` INT,
# MAGIC   `END_OF_CAL_QUARTER` STRING,
# MAGIC   `END_OF_FIS_QUARTER` STRING,
# MAGIC   `CALENDAR_QUARTER_NUMBER` INT,
# MAGIC   `FISCAL_QUARTER_NUMBER` INT,
# MAGIC   `CALENDAR_YEAR` INT,
# MAGIC   `CALENDAR_YEAR_ID` INT,
# MAGIC   `FISCAL_YEAR` INT,
# MAGIC   `FISCAL_YEAR_ID` INT,
# MAGIC   `DAYS_IN_CAL_YEAR` INT,
# MAGIC   `DAYS_IN_FIS_YEAR` INT,
# MAGIC   `END_OF_CAL_YEAR` STRING,
# MAGIC   `END_OF_FIS_YEAR` STRING,
# MAGIC   CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING, INPUT_FILE STRING)
# MAGIC USING delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /***************************************************************
# MAGIC  CREATE TABLE DDL Scirpts for Curation tables
# MAGIC ****************************************************************/
# MAGIC USE CURATION_SALES;
# MAGIC
# MAGIC DROP TABLE IF EXISTS `CURATION_SALES`.`curation_sales_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `CURATION_SALES`.`curation_sales_transaction` ( `PROD_ID` INT, `CUST_ID` INT, `TIME_ID` STRING, `CHANNEL_ID` INT, `PROMO_ID` INT, `QUANTITY_SOLD` DOUBLE, `AMOUNT_SOLD` DOUBLE,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `CURATION_SALES`.`curation_costs_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `CURATION_SALES`.`curation_costs_transaction` ( `PROD_ID` INT, `TIME_ID` STRING, `PROMO_ID` INT, `CHANNEL_ID` INT, `UNIT_COST` DOUBLE, `UNIT_PRICE` DOUBLE, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `CURATION_SALES`.`curation_product`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  `CURATION_SALES`.`curation_product` ( `PROD_ID` INT, `PROD_NAME` STRING, `PROD_DESC` STRING, `PROD_SUBCATEGORY` STRING, `PROD_SUBCATEGORY_ID` STRING, `PROD_SUBCATEGORY_DESC` STRING, `PROD_CATEGORY` STRING, `PROD_CATEGORY_ID` STRING, `PROD_CATEGORY_DESC` STRING, `PROD_WEIGHT_CLASS` STRING, `PROD_UNIT_OF_MEASURE` STRING, `PROD_PACK_SIZE` STRING, `SUPPLIER_ID` STRING, `PROD_STATUS` STRING, `PROD_LIST_PRICE` STRING, `PROD_MIN_PRICE` STRING, `PROD_TOTAL` STRING, `PROD_TOTAL_ID` STRING, `PROD_SRC_ID` STRING, `PROD_EFF_FROM` STRING, `PROD_EFF_TO` STRING, `PROD_VALID` STRING,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC DROP TABLE IF  EXISTS  `CURATION_SALES`.`curation_countries`;
# MAGIC CREATE TABLE IF NOT EXISTS  `CURATION_SALES`.`curation_countries` ( `COUNTRY_ID` INT, `COUNTRY_ISO_CODE` STRING, `COUNTRY_NAME` STRING, `COUNTRY_SUBREGION` STRING, `COUNTRY_SUBREGION_ID` INT, `COUNTRY_REGION` STRING, `COUNTRY_REGION_ID` INT, `COUNTRY_TOTAL` STRING, `COUNTRY_TOTAL_ID` INT, `COUNTRY_NAME_HIST` STRING,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `CURATION_SALES`.`curation_customers`;
# MAGIC CREATE TABLE IF NOT EXISTS  `CURATION_SALES`.`curation_customers` ( `CUST_ID` INT, `CUST_FIRST_NAME` STRING, `CUST_LAST_NAME` STRING, `CUST_GENDER` STRING, `CUST_YEAR_OF_BIRTH` INT, `CUST_MARITAL_STATUS` STRING, `CUST_STREET_ADDRESS` STRING, `CUST_POSTAL_CODE` INT, `CUST_CITY` STRING, `CUST_CITY_ID` STRING, `CUST_STATE_PROVINCE` STRING, `CUST_STATE_PROVINCE_ID` STRING, `COUNTRY_ID` INT, `CUST_MAIN_PHONE_NUMBER` STRING, `CUST_INCOME_LEVEL` STRING, `CUST_CREDIT_LIMIT` STRING, `CUST_EMAIL` STRING, `CUST_TOTAL` STRING, `CUST_TOTAL_ID` STRING, `CUST_SRC_ID` STRING, `CUST_EFF_FROM` STRING, `CUST_EFF_TO` STRING, `CUST_VALID` STRING, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC DROP TABLE IF EXISTS `CURATION_SALES`.`curation_channels`;
# MAGIC CREATE TABLE IF NOT EXISTS `CURATION_SALES`.`curation_channels` ( `CHANNEL_ID` INT, `CHANNEL_DESC` STRING, `CHANNEL_CLASS` STRING, `CHANNEL_CLASS_ID` INT, `CHANNEL_TOTAL` STRING, `CHANNEL_TOTAL_ID` INT,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `CURATION_SALES`.`curation_promotions`;
# MAGIC CREATE TABLE IF NOT EXISTS `CURATION_SALES`.`curation_promotions` ( `PROMO_ID` INT, `PROMO_NAME` STRING, `PROMO_SUBCATEGORY` STRING, `PROMO_SUBCATEGORY_ID` INT, `PROMO_CATEGORY` STRING, `PROMO_CATEGORY_ID` INT,
# MAGIC `PROMO_COST` INT,
# MAGIC `PROMO_BEGIN_DATE` STRING,
# MAGIC `PROMO_END_DATE` STRING,
# MAGIC `PROMO_TOTAL` STRING,
# MAGIC `PROMO_TOTAL_ID` INT, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC DROP TABLE IF  EXISTS  `CURATION_SALES`.`curation_times`;
# MAGIC CREATE TABLE IF NOT EXISTS  `CURATION_SALES`.`curation_times` (
# MAGIC   `TIME_ID` STRING,
# MAGIC   `DAY_NAME` STRING,
# MAGIC   `DAY_NUMBER_IN_WEEK` INT,
# MAGIC   `DAY_NUMBER_IN_MONTH` INT,
# MAGIC   `CALENDAR_WEEK_NUMBER` INT,
# MAGIC   `FISCAL_WEEK_NUMBER` INT,
# MAGIC   `WEEK_ENDING_DAY` STRING,
# MAGIC   `WEEK_ENDING_DAY_ID` INT,
# MAGIC   `CALENDAR_MONTH_NUMBER` INT,
# MAGIC   `FISCAL_MONTH_NUMBER` INT,
# MAGIC   `CALENDAR_MONTH_DESC` STRING,
# MAGIC   `CALENDAR_MONTH_ID` INT,
# MAGIC   `FISCAL_MONTH_DESC` STRING,
# MAGIC   `FISCAL_MONTH_ID` INT,
# MAGIC   `DAYS_IN_CAL_MONTH` INT,
# MAGIC   `DAYS_IN_FIS_MONTH` INT,
# MAGIC   `END_OF_CAL_MONTH` STRING,
# MAGIC   `END_OF_FIS_MONTH` STRING,
# MAGIC   `CALENDAR_MONTH_NAME` STRING,
# MAGIC   `FISCAL_MONTH_NAME` STRING,
# MAGIC   `CALENDAR_QUARTER_DESC` STRING,
# MAGIC   `CALENDAR_QUARTER_ID` INT,
# MAGIC   `FISCAL_QUARTER_DESC` STRING,
# MAGIC   `FISCAL_QUARTER_ID` INT,
# MAGIC   `DAYS_IN_CAL_QUARTER` INT,
# MAGIC   `DAYS_IN_FIS_QUARTER` INT,
# MAGIC   `END_OF_CAL_QUARTER` STRING,
# MAGIC   `END_OF_FIS_QUARTER` STRING,
# MAGIC   `CALENDAR_QUARTER_NUMBER` INT,
# MAGIC   `FISCAL_QUARTER_NUMBER` INT,
# MAGIC   `CALENDAR_YEAR` INT,
# MAGIC   `CALENDAR_YEAR_ID` INT,
# MAGIC   `FISCAL_YEAR` INT,
# MAGIC   `FISCAL_YEAR_ID` INT,
# MAGIC   `DAYS_IN_CAL_YEAR` INT,
# MAGIC   `DAYS_IN_FIS_YEAR` INT,
# MAGIC   `END_OF_CAL_YEAR` STRING,
# MAGIC   `END_OF_FIS_YEAR` STRING,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING)
# MAGIC USING delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /***************************************************************
# MAGIC  CREATE TABLE DDL Scirpts for Target Warehouse tables
# MAGIC ****************************************************************/
# MAGIC USE DW_SALES;
# MAGIC
# MAGIC DROP TABLE IF EXISTS `DW_SALES`.`fact_sales_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `DW_SALES`.`fact_sales_transaction` (SALES_FACT_KEY  STRING, `PROD_KEY` STRING, `CUST_KEY` STRING, `TIME_KEY` STRING, `CHANNEL_KEY` STRING, `PROMO_KEY` STRING, `QUANTITY_SOLD` DOUBLE, `AMOUNT_SOLD` DOUBLE,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `DW_SALES`.`fact_costs_transaction`;
# MAGIC CREATE TABLE IF NOT EXISTS `DW_SALES`.`fact_costs_transaction` ( COST_FACT_KEY STRING,`PROD_KEY` STRING, `TIME_KEY` STRING, `PROMO_KEY` STRING, `CHANNEL_KEY` STRING, `UNIT_COST` DOUBLE, `UNIT_PRICE` DOUBLE, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `DW_SALES`.`dim_product`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  `DW_SALES`.`dim_product` ( `PROD_KEY` STRING,`PROD_ID` INT, `PROD_NAME` STRING, `PROD_DESC` STRING, `PROD_SUBCATEGORY` STRING, `PROD_SUBCATEGORY_ID` STRING, `PROD_SUBCATEGORY_DESC` STRING, `PROD_CATEGORY` STRING, `PROD_CATEGORY_ID` STRING, `PROD_CATEGORY_DESC` STRING, `PROD_WEIGHT_CLASS` STRING, `PROD_UNIT_OF_MEASURE` STRING, `PROD_PACK_SIZE` STRING, `SUPPLIER_ID` STRING, `PROD_STATUS` STRING, `PROD_LIST_PRICE` STRING, `PROD_MIN_PRICE` STRING, `PROD_TOTAL` STRING, `PROD_TOTAL_ID` STRING, `PROD_SRC_ID` STRING, `PROD_EFF_FROM` STRING, `PROD_EFF_TO` STRING, `PROD_VALID` STRING,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `DW_SALES`.`dim_countries`;
# MAGIC CREATE TABLE IF NOT EXISTS  `DW_SALES`.`dim_countries` ( `COUNTRY_KEY` STRING,`COUNTRY_ID` INT, `COUNTRY_ISO_CODE` STRING, `COUNTRY_NAME` STRING, `COUNTRY_SUBREGION` STRING, `COUNTRY_SUBREGION_ID` INT, `COUNTRY_REGION` STRING, `COUNTRY_REGION_ID` INT, `COUNTRY_TOTAL` STRING, `COUNTRY_TOTAL_ID` INT, `COUNTRY_NAME_HIST` STRING,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `DW_SALES`.`dim_customers`;
# MAGIC CREATE TABLE IF NOT EXISTS  `DW_SALES`.`dim_customers` ( `CUST_KEY` STRING,`CUST_ID` INT, `CUST_FIRST_NAME` STRING, `CUST_LAST_NAME` STRING, `CUST_GENDER` STRING, `CUST_YEAR_OF_BIRTH` INT, `CUST_MARITAL_STATUS` STRING, `CUST_STREET_ADDRESS` STRING, `CUST_POSTAL_CODE` INT, `CUST_CITY` STRING, `CUST_CITY_ID` STRING, `CUST_STATE_PROVINCE` STRING, `CUST_STATE_PROVINCE_ID` STRING, `COUNTRY_ID` INT, `CUST_MAIN_PHONE_NUMBER` STRING, `CUST_INCOME_LEVEL` STRING, `CUST_CREDIT_LIMIT` STRING, `CUST_EMAIL` STRING, `CUST_TOTAL` STRING, `CUST_TOTAL_ID` STRING, `CUST_SRC_ID` STRING, `CUST_EFF_FROM` STRING, `CUST_EFF_TO` STRING, `CUST_VALID` STRING, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF EXISTS `DW_SALES`.`dim_channels`;
# MAGIC CREATE TABLE IF NOT EXISTS `DW_SALES`.`dim_channels` ( `CHANNEL_KEY` STRING,`CHANNEL_ID` INT, `CHANNEL_DESC` STRING, `CHANNEL_CLASS` STRING, `CHANNEL_CLASS_ID` INT, `CHANNEL_TOTAL` STRING, `CHANNEL_TOTAL_ID` INT,  CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS `DW_SALES`.`dim_promotions`;
# MAGIC CREATE TABLE IF NOT EXISTS `DW_SALES`.`dim_promotions` (`PROMO_KEY` STRING, `PROMO_ID` INT, `PROMO_NAME` STRING, `PROMO_SUBCATEGORY` STRING, `PROMO_SUBCATEGORY_ID` INT, `PROMO_CATEGORY` STRING, `PROMO_CATEGORY_ID` INT,
# MAGIC `PROMO_COST` INT,
# MAGIC `PROMO_BEGIN_DATE` STRING,
# MAGIC `PROMO_END_DATE` STRING,
# MAGIC `PROMO_TOTAL` STRING,
# MAGIC `PROMO_TOTAL_ID` INT, CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING) USING delta;
# MAGIC
# MAGIC DROP TABLE IF  EXISTS  `DW_SALES`.`dim_times`;
# MAGIC CREATE TABLE IF NOT EXISTS  `DW_SALES`.`dim_times` (
# MAGIC   `TIME_KEY` STRING,
# MAGIC   `TIME_ID` STRING,
# MAGIC   `DAY_NAME` STRING,
# MAGIC   `DAY_NUMBER_IN_WEEK` INT,
# MAGIC   `DAY_NUMBER_IN_MONTH` INT,
# MAGIC   `CALENDAR_WEEK_NUMBER` INT,
# MAGIC   `FISCAL_WEEK_NUMBER` INT,
# MAGIC   `WEEK_ENDING_DAY` STRING,
# MAGIC   `WEEK_ENDING_DAY_ID` INT,
# MAGIC   `CALENDAR_MONTH_NUMBER` INT,
# MAGIC   `FISCAL_MONTH_NUMBER` INT,
# MAGIC   `CALENDAR_MONTH_DESC` STRING,
# MAGIC   `CALENDAR_MONTH_ID` INT,
# MAGIC   `FISCAL_MONTH_DESC` STRING,
# MAGIC   `FISCAL_MONTH_ID` INT,
# MAGIC   `DAYS_IN_CAL_MONTH` INT,
# MAGIC   `DAYS_IN_FIS_MONTH` INT,
# MAGIC   `END_OF_CAL_MONTH` STRING,
# MAGIC   `END_OF_FIS_MONTH` STRING,
# MAGIC   `CALENDAR_MONTH_NAME` STRING,
# MAGIC   `FISCAL_MONTH_NAME` STRING,
# MAGIC   `CALENDAR_QUARTER_DESC` STRING,
# MAGIC   `CALENDAR_QUARTER_ID` INT,
# MAGIC   `FISCAL_QUARTER_DESC` STRING,
# MAGIC   `FISCAL_QUARTER_ID` INT,
# MAGIC   `DAYS_IN_CAL_QUARTER` INT,
# MAGIC   `DAYS_IN_FIS_QUARTER` INT,
# MAGIC   `END_OF_CAL_QUARTER` STRING,
# MAGIC   `END_OF_FIS_QUARTER` STRING,
# MAGIC   `CALENDAR_QUARTER_NUMBER` INT,
# MAGIC   `FISCAL_QUARTER_NUMBER` INT,
# MAGIC   `CALENDAR_YEAR` INT,
# MAGIC   `CALENDAR_YEAR_ID` INT,
# MAGIC   `FISCAL_YEAR` INT,
# MAGIC   `FISCAL_YEAR_ID` INT,
# MAGIC   `DAYS_IN_CAL_YEAR` INT,
# MAGIC   `DAYS_IN_FIS_YEAR` INT,
# MAGIC   `END_OF_CAL_YEAR` STRING,
# MAGIC   `END_OF_FIS_YEAR` STRING,CREATED_DATE TIMESTAMP,CREATED_BY STRING,UPDATED_DATE TIMESTAMP, UPDATED_BY STRING)
# MAGIC USING delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF  NOT EXISTS JOBS;
# MAGIC USE JOBS;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS JOBS.JOB_LIST_DYNAMIC;
# MAGIC CREATE TABLE IF NOT EXISTS JOBS.JOB_LIST_DYNAMIC(
# MAGIC   job_id varchar(100),
# MAGIC   job_name varchar(100),
# MAGIC   source_database_host varchar (200),
# MAGIC   source_database_username varchar (200),
# MAGIC   source_database_password varchar (200),
# MAGIC   source_database_name varchar (200),
# MAGIC   source_schema_name varchar (200),
# MAGIC   source_table_name varchar (200),
# MAGIC   source_file_path varchar (200),
# MAGIC   source_file_username varchar (200),
# MAGIC   source_file_password varchar (200),
# MAGIC   source_folder_name varchar (200),
# MAGIC   source_file_name varchar (200),
# MAGIC   source_file_type varchar (200),
# MAGIC   landing_zone_file_path varchar (200),
# MAGIC   landing_zone_folder_name varchar (200),
# MAGIC   landing_zone_file_name varchar (200),
# MAGIC   landing_zone_file_type varchar (200),
# MAGIC   staging_zone_database_name varchar (200),
# MAGIC   staging_zone_schema_name varchar (200),
# MAGIC   staging_zone_table_name varchar (200),
# MAGIC   staging_zone_table_pk_column varchar (200),
# MAGIC   curation_zone_database_name varchar (200),
# MAGIC   curation_zone_schema_name varchar (200),
# MAGIC   curation_zone_table_name varchar (200),
# MAGIC   curation_zone_table_pk_column varchar (200),
# MAGIC   dw_zone_database_name varchar (200),
# MAGIC   dw_zone_schema_name varchar (200),
# MAGIC   dw_zone_table_name varchar (200),
# MAGIC   dw_zone_table_pk_column varchar (200),
# MAGIC   raw_zone_file_path varchar (200),
# MAGIC   raw_zone_folder_name varchar (200),
# MAGIC   raw_zone_file_name varchar (200),
# MAGIC   raw_zone_file_type varchar (200),
# MAGIC   pyspark_schema varchar(200),
# MAGIC   table_type varchar(20),
# MAGIC   job_type varchar(20),
# MAGIC   job_status varchar(20),
# MAGIC   watermark TIMESTAMP,
# MAGIC   created_on TIMESTAMP,
# MAGIC   created_by varchar(200),
# MAGIC   updated_on TIMESTAMP,
# MAGIC   updated_by varchar(200)
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     1,
# MAGIC     'countries',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'countries',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'countries',
# MAGIC     'countries',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'countries',
# MAGIC     'countries',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_countries',
# MAGIC     'COUNTRY_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_countries',
# MAGIC     'COUNTRY_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_countries',
# MAGIC     'COUNTRY_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'countries',
# MAGIC     'countries',
# MAGIC     'csv',
# MAGIC     'countries_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     2,
# MAGIC     'channels',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'channels',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'channels',
# MAGIC     'channels',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'channels',
# MAGIC     'channels',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_channels',
# MAGIC     'CHANNEL_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_channels',
# MAGIC     'CHANNEL_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_channels',
# MAGIC     'CHANNEL_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'channels',
# MAGIC     'channels',
# MAGIC     'csv',
# MAGIC     'channels_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     3,
# MAGIC     'customers',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'customers',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'customers',
# MAGIC     'customers',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'customers',
# MAGIC     'customers',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_customers',
# MAGIC     'CUST_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_customers',
# MAGIC     'CUST_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_customers',
# MAGIC     'CUST_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'customers',
# MAGIC     'customers',
# MAGIC     'csv',
# MAGIC     'customers_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     4,
# MAGIC     'product',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'product',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'product',
# MAGIC     'product',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'product',
# MAGIC     'product',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_product',
# MAGIC     'PROD_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_product',
# MAGIC     'PROD_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_product',
# MAGIC     'PROD_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'product',
# MAGIC     'product',
# MAGIC     'csv',
# MAGIC     'product_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     5,
# MAGIC     'promotions',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'promotions',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'promotions',
# MAGIC     'promotions',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'promotions',
# MAGIC     'promotions',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_promotions',
# MAGIC     'PROMO_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_promotions',
# MAGIC     'PROMO_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_promotions',
# MAGIC     'PROMO_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'promotions',
# MAGIC     'promotions',
# MAGIC     'csv',
# MAGIC     'promotions_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     6,
# MAGIC     'times',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'times',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'times',
# MAGIC     'times',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'times',
# MAGIC     'times',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_times',
# MAGIC     'TIME_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_times',
# MAGIC     'TIME_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'dim_times',
# MAGIC     'TIME_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'times',
# MAGIC     'times',
# MAGIC     'csv',
# MAGIC     'times_schema',
# MAGIC     'dim',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     7,
# MAGIC     'sales_transaction',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'sales_transaction',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'sales_transaction',
# MAGIC     'sales_transaction',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'sales_transaction',
# MAGIC     'sales_transaction',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_sales_transaction',
# MAGIC     'PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_sales_transaction',
# MAGIC     'PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'fact_sales_transaction',
# MAGIC     'SALES_FACT_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'sales_transaction',
# MAGIC     'sales_transaction',
# MAGIC     'csv',
# MAGIC     'sales_schema',
# MAGIC     'fact',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );
# MAGIC insert into
# MAGIC   JOB_LIST_DYNAMIC
# MAGIC values(
# MAGIC     8,
# MAGIC     'costs_transaction',
# MAGIC     'localhost',
# MAGIC     'sales_read',
# MAGIC     'sales_read',
# MAGIC     'sh',
# MAGIC     'sh',
# MAGIC     'costs_transaction',
# MAGIC     'E:/files/SH/',
# MAGIC     '1234',
# MAGIC     'password',
# MAGIC     'costs_transaction',
# MAGIC     'costs_transaction',
# MAGIC     'csv',
# MAGIC     '/mnt/landing/sales/',
# MAGIC     'costs_transaction',
# MAGIC     'costs_transaction',
# MAGIC     'csv',
# MAGIC     'stg_sales',
# MAGIC     'sales',
# MAGIC     'stg_costs_transaction',
# MAGIC     'PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID',
# MAGIC     'curation_sales',
# MAGIC     'sales',
# MAGIC     'curation_costs_transaction',
# MAGIC     'PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID',
# MAGIC     'dw_sales',
# MAGIC     'sales',
# MAGIC     'fact_costs_transaction',
# MAGIC     'COST_FACT_KEY',
# MAGIC     '/mnt/raw/sales/',
# MAGIC     'costs_transaction',
# MAGIC     'costs_transaction',
# MAGIC     'csv',
# MAGIC     'costs_schema',
# MAGIC     'fact',
# MAGIC     'incremental',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB',
# MAGIC     current_timestamp(),
# MAGIC     'ETLJOB'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC update jobs.JOB_LIST_DYNAMIC
# MAGIC set landing_zone_file_path='/mnt/landing/sales/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jobs.JOB_LIST_DYNAMIC where  job_status=1 order by job_id
# MAGIC -- sql_query  

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS JOBS.pipeline_log;
# MAGIC CREATE TABLE IF NOT EXISTS JOBS.pipeline_log(
# MAGIC    log_id STRING ,
# MAGIC    job_id STRING ,
# MAGIC    datafactory_name STRING,
# MAGIC    pipeline_name STRING,
# MAGIC    pipeline_runid STRING,
# MAGIC    triggertype STRING,
# MAGIC    triggerid STRING,
# MAGIC    triggername STRING,
# MAGIC    triggertime STRING,
# MAGIC    landing_rows STRING,
# MAGIC    rejected_rows STRING,
# MAGIC    staging_rows string,
# MAGIC    duplicate_rows STRING,
# MAGIC    curation_read_rows string,
# MAGIC    curation_inserted_rows string,
# MAGIC    curation_updated_rows string,
# MAGIC    dwh_read_rows string,
# MAGIC    dwh_inserted_rows string,
# MAGIC    dwh_updated_rows string,
# MAGIC    no_parallelcopies STRING,
# MAGIC    copyduration_in_secs STRING,
# MAGIC    effectiveintegrationruntime STRING,
# MAGIC    source_type STRING,
# MAGIC    sink_type STRING,
# MAGIC    execution_status STRING,
# MAGIC    staging_stage_start_time STRING,
# MAGIC    staging_stage_end_time STRING,
# MAGIC    curation_stage_start_time STRING,
# MAGIC    curation_stage_end_time STRING,
# MAGIC    dwh_stage_start_time STRING,
# MAGIC    dwh_stage_end_time STRING,
# MAGIC    copyactivity_queuingduration_in_secs STRING,
# MAGIC    copyactivity_transferduration_in_secs STRING) USING DELTA;
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from JOBS.pipeline_log
