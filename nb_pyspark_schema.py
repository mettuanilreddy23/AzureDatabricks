# Databricks notebook source
# MAGIC %md
# MAGIC ### Pyspark Data types and its length
# MAGIC * All datatypes we can import from  __`from pyspark.sql.types import *`__

# COMMAND ----------

# MAGIC %md
# MAGIC <div class="compound-middle compound clt-tabs-content" lang="r" i=
# MAGIC d="0-language-r" data-lang="language-r" aria-controlled-by="0-anchor-=
# MAGIC language-r" role="tabpanel" aria-hidden="true">
# MAGIC <div class="wy-table-responsive"><table border="1" class="docutils">
# MAGIC <colgroup>
# MAGIC <col width="3%">
# MAGIC <col width="60%">
# MAGIC <col width="37%">
# MAGIC </colgroup>
# MAGIC <thead valign="bottom">
# MAGIC <tr class="row-odd"><th class="head">Data type</th>
# MAGIC <th class="head">Value type</th>
# MAGIC <th class="head">API to access or create data type</th>
# MAGIC </tr>
# MAGIC </thead>
# MAGIC <tbody valign="top">
# MAGIC <tr class="row-even"><td>ByteType</td>
# MAGIC <td>integer  <strong>Note:</strong> Numbers are converted to 1-byte signed =
# MAGIC integer numbers at runtime.  Make sure sure that numbers are within the ran=
# MAGIC ge of -128 to 127.</td>
# MAGIC <td>ByteType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>ShortType</td>
# MAGIC <td>integer  <strong>Note:</strong> Numbers are converted to 2-byte signed =
# MAGIC integer numbers at runtime.  Make sure sure that numbers are within the ran=
# MAGIC ge of -32768 to 32767.</td>
# MAGIC <td>ShortType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>IntegerType</td>
# MAGIC <td>integer</td>
# MAGIC <td>IntegerType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>LongType</td>
# MAGIC <td>integer  <strong>Note:</strong> Numbers are converted to 8-byte signed =
# MAGIC integer numbers at runtime.  Make sure sure that numbers are within the ran=
# MAGIC ge of -9223372036854775808 to 9223372036854775807.  Otherwise, convert data=
# MAGIC  to decimal.Decimal and use DecimalType.</td>
# MAGIC <td>LongType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>FloatType</td>
# MAGIC <td>numeric  <strong>Note:</strong> Numbers are converted to 4-byte single-=
# MAGIC precision floating point numbers at runtime.</td>
# MAGIC <td>FloatType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>DoubleType</td>
# MAGIC <td>float</td>
# MAGIC <td>DobleType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>DecimalType</td>
# MAGIC <td>decimal.Decimal</td>
# MAGIC <td>DecimalType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>StringType</td>
# MAGIC <td>character</td>
# MAGIC <td>StringType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>BinaryType</td>
# MAGIC <td>raw</td>
# MAGIC <td>BinaryType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>BooleanType</td>
# MAGIC <td>logical</td>
# MAGIC <td>BooleanType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>TimestampType</td>
# MAGIC <td>datetime.datetime</td>
# MAGIC <td>TimestampType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>DateType</td>
# MAGIC <td>datetime.date</td>
# MAGIC <td>DateType()</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>ArrayType</td>
# MAGIC <td>vector or list</td>
# MAGIC <td>list(type=array, elementType=elementType, contain=
# MAGIC sNull=[containsNull]) <strong>Note:</strong> The default value of contain=
# MAGIC sNull is TRUE.</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>MapType</td>
# MAGIC <td>environment</td>
# MAGIC <td>list(type=map, keyType=keyType, valueType=value=
# MAGIC Type, valueContainsNull=[valueContainsNull]) <strong>Note:</strong> The d=
# MAGIC efault value of valueContainsNull is TRUE.</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>StructType</td>
# MAGIC <td>named list</td>
# MAGIC <td>list(type struct, fields=fields) <strong>Note:</s=trong> fields is a Seq of StructFields. Also, two fields with the same name=
# MAGIC  are not allowed.</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>StructField</td>
# MAGIC <td>The value type of the data type of this field (For example, integer for=
# MAGIC  a StructField with the data type IntegerType)</td>
# MAGIC <td>list(name=name, type=dataType, nullable=[nullable]) <strong>Note:=
# MAGIC </strong> The default value of nullable is TRUE.</td>
# MAGIC </tr>
# MAGIC </tbody>
# MAGIC </table></div>
# MAGIC </div>

# COMMAND ----------

#**************************************************
#* Importing pyspark required Data Types 
#***************************************************/
from pyspark.sql.types import StructField, StructType, StringType,LongType,IntegerType,DoubleType

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For Channel Table
#***************************************************/
def channels_schema():
  channels_schema = StructType([StructField('CHANNEL_ID', IntegerType(), False),
                     StructField('CHANNEL_DESC', StringType(), False),
                     StructField('CHANNEL_CLASS', StringType(), False),
                     StructField('CHANNEL_CLASS_ID', IntegerType(), False),
                     StructField('CHANNEL_TOTAL', StringType(), False),
                     StructField('CHANNEL_TOTAL_ID', IntegerType(), False),
                     StructField('_corrupt_record', StringType(), True)])
  return channels_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For Countries Table
#***************************************************/
def countries_schema():
  countries_schema = StructType([StructField('COUNTRY_ID', IntegerType(), False),
                     StructField('COUNTRY_ISO_CODE', StringType(), False),
                     StructField('COUNTRY_NAME', StringType(), False),
                     StructField('COUNTRY_SUBREGION', StringType(), False),
                     StructField('COUNTRY_SUBREGION_ID', IntegerType(), False),
                     StructField('COUNTRY_REGION', StringType(), False),
                     StructField('COUNTRY_REGION_ID', IntegerType(), False),
					 StructField('COUNTRY_TOTAL', StringType(), False),
					 StructField('COUNTRY_TOTAL_ID', IntegerType(), False),
					 StructField('COUNTRY_NAME_HIST', StringType(), False),
                     StructField('_corrupt_record', StringType(), False)])
  return countries_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For Customers Table
#***************************************************
def customers_schema():
  customers_schema = StructType([StructField('CUST_ID', IntegerType(), False),
                     StructField('CUST_FIRST_NAME', StringType(), False),
                     StructField('CUST_LAST_NAME', StringType(), False),
                     StructField('CUST_GENDER', StringType(), False),
                     StructField('CUST_YEAR_OF_BIRTH', IntegerType(), False),
                     StructField('CUST_MARITAL_STATUS', StringType(), False),
                     StructField('CUST_STREET_ADDRESS', StringType(), False),
					 StructField('CUST_POSTAL_CODE', IntegerType(), False),
					 StructField('CUST_CITY', StringType(), False),
					 StructField('CUST_CITY_ID', StringType(), False),
					 StructField('CUST_STATE_PROVINCE', StringType(), False),
					 StructField('CUST_STATE_PROVINCE_ID', StringType(), False),
					 StructField('COUNTRY_ID', IntegerType(), False),
					 StructField('CUST_MAIN_PHONE_NUMBER', StringType(), False),
					 StructField('CUST_INCOME_LEVEL', StringType(), False),
					 StructField('CUST_CREDIT_LIMIT', StringType(), False),
					 StructField('CUST_EMAIL', StringType(), False),
					 StructField('CUST_TOTAL', StringType(), False),
					 StructField('CUST_TOTAL_ID', StringType(), False),
					 StructField('CUST_SRC_ID', StringType(), False),
					 StructField('CUST_EFF_FROM', StringType(), False),
					 StructField('CUST_EFF_TO', StringType(), False),
					 StructField('CUST_VALID', StringType(), False),
                     StructField('_corrupt_record', StringType(), False)])
  return customers_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For product Table
#***************************************************
def product_schema():
  product_schema = StructType([StructField('PROD_ID', IntegerType(), False),
                     StructField('PROD_NAME', StringType(), False),
                     StructField('PROD_DESC', StringType(), False),
                     StructField('PROD_SUBCATEGORY', StringType(), False),
                     StructField('PROD_SUBCATEGORY_ID', StringType(), False),
                     StructField('PROD_SUBCATEGORY_DESC', StringType(), False),
                     StructField('PROD_CATEGORY', StringType(), False),
					 StructField('PROD_CATEGORY_ID', StringType(), False),
					 StructField('PROD_CATEGORY_DESC', StringType(), False),
					 StructField('PROD_WEIGHT_CLASS', StringType(), False),
					 StructField('PROD_UNIT_OF_MEASURE', StringType(), False),
					 StructField('PROD_PACK_SIZE', StringType(), False),
					 StructField('SUPPLIER_ID', StringType(), False),
					 StructField('PROD_STATUS', StringType(), False),
					 StructField('PROD_LIST_PRICE', StringType(), False),
					 StructField('PROD_MIN_PRICE', StringType(), False),
					 StructField('PROD_TOTAL', StringType(), False),
					 StructField('PROD_TOTAL_ID', StringType(), False),
					 StructField('PROD_SRC_ID', StringType(), False),
					 StructField('PROD_EFF_FROM', StringType(), False),
					 StructField('PROD_EFF_TO', StringType(), False),
					 StructField('PROD_VALID', StringType(), False),
                     StructField('_corrupt_record', StringType(), False)])
  return product_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For promotions Table
#***************************************************
def promotions_schema():
  promotions_schema = StructType([StructField('PROMO_ID', IntegerType(), False),
                     StructField('PROMO_NAME', StringType(), False),
                     StructField('PROMO_SUBCATEGORY', StringType(), False),
                     StructField('PROMO_SUBCATEGORY_ID', IntegerType(), False),
                     StructField('PROMO_CATEGORY', StringType(), False),
                     StructField('PROMO_CATEGORY_ID', IntegerType(), False),
                     StructField('PROMO_COST', IntegerType(), False),
                     StructField('PROMO_BEGIN_DATE', StringType(), False),
                     StructField('PROMO_END_DATE', StringType(), False),
                     StructField('PROMO_TOTAL', StringType(), False),
                     StructField('PROMO_TOTAL_ID', IntegerType(), False),
                     StructField('_corrupt_record', StringType(), False)])
  return promotions_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For times Table
#***************************************************
def times_schema():
  times_schema = StructType([StructField('TIME_ID',StringType(),True),
                           StructField('DAY_NAME',StringType(),True),
                           StructField('DAY_NUMBER_IN_WEEK',IntegerType(),True),
                           StructField('DAY_NUMBER_IN_MONTH',IntegerType(),True),
                           StructField('CALENDAR_WEEK_NUMBER',IntegerType(),True),
                           StructField('FISCAL_WEEK_NUMBER',IntegerType(),True),
                           StructField('WEEK_ENDING_DAY',StringType(),True),
                           StructField('WEEK_ENDING_DAY_ID',IntegerType(),True),
                           StructField('CALENDAR_MONTH_NUMBER',IntegerType(),True),
                           StructField('FISCAL_MONTH_NUMBER',IntegerType(),True),
                           StructField('CALENDAR_MONTH_DESC',StringType(),True),
                           StructField('CALENDAR_MONTH_ID',IntegerType(),True),
                           StructField('FISCAL_MONTH_DESC',StringType(),True),
                           StructField('FISCAL_MONTH_ID',IntegerType(),True),
                           StructField('DAYS_IN_CAL_MONTH',IntegerType(),True),
                           StructField('DAYS_IN_FIS_MONTH',IntegerType(),True),
                           StructField('END_OF_CAL_MONTH',StringType(),True),
                           StructField('END_OF_FIS_MONTH',StringType(),True),
                           StructField('CALENDAR_MONTH_NAME',StringType(),True),
                           StructField('FISCAL_MONTH_NAME',StringType(),True),
                           StructField('CALENDAR_QUARTER_DESC',StringType(),True),
                           StructField('CALENDAR_QUARTER_ID',IntegerType(),True),
                           StructField('FISCAL_QUARTER_DESC',StringType(),True),
                           StructField('FISCAL_QUARTER_ID',IntegerType(),True),
                           StructField('DAYS_IN_CAL_QUARTER',IntegerType(),True),
                           StructField('DAYS_IN_FIS_QUARTER',IntegerType(),True),
                           StructField('END_OF_CAL_QUARTER',StringType(),True),
                           StructField('END_OF_FIS_QUARTER',StringType(),True),
                           StructField('CALENDAR_QUARTER_NUMBER',IntegerType(),True),
                           StructField('FISCAL_QUARTER_NUMBER',IntegerType(),True),
                           StructField('CALENDAR_YEAR',IntegerType(),True),
                           StructField('CALENDAR_YEAR_ID',IntegerType(),True),
                           StructField('FISCAL_YEAR',IntegerType(),True),
                           StructField('FISCAL_YEAR_ID',IntegerType(),True),
                           StructField('DAYS_IN_CAL_YEAR',IntegerType(),True),
                           StructField('DAYS_IN_FIS_YEAR',IntegerType(),True),
                           StructField('END_OF_CAL_YEAR',StringType(),True),
                           StructField('END_OF_FIS_YEAR',StringType(),True),
                           StructField('_corrupt_record', StringType(), False)])
  return times_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For sales transaction Table
#***************************************************
def sales_schema():
  sales_schema = StructType([StructField('PROD_ID', IntegerType(), True),
                     StructField('CUST_ID', IntegerType(), True),
                     StructField('TIME_ID', StringType(), True),
                     StructField('CHANNEL_ID', IntegerType(), True),
                     StructField('PROMO_ID', IntegerType(), True),
                     StructField('QUANTITY_SOLD', DoubleType(), True),
                     StructField('AMOUNT_SOLD', DoubleType(), True),
                     StructField('_corrupt_record', StringType(), True)])
  return sales_schema

# COMMAND ----------

#**************************************************
#* Creating Pyspark Schema For costs transaction Table
#***************************************************
def costs_schema():
  costs_schema = StructType([StructField('PROD_ID',IntegerType(),True),
                               StructField('TIME_ID',StringType(),True),
                               StructField('PROMO_ID',IntegerType(),True),
                               StructField('CHANNEL_ID',IntegerType(),True),
                               StructField('UNIT_COST',DoubleType(),True),
                               StructField('UNIT_PRICE',DoubleType(),True),
                               StructField('_corrupt_record', StringType(), True)])
  return costs_schema
