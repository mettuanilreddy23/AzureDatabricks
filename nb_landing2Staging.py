# Databricks notebook source
# MAGIC %md 
# MAGIC ###### Schemas for the tables 

# COMMAND ----------

# MAGIC %run "/Repos/mettuanilreddy@yahoo.co.in/repo_e2e_project/nb_pyspark_schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### landing files mount point creation

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://landing-files@adlsstorageaccount85.blob.core.windows.net",
    mount_point = "/mnt/landing-files/",
    extra_configs = {"fs.azure.account.key.adlsstorageaccount85.blob.core.windows.net":"NKpIt6TXc5psjZWH5GEqYpZzakWP2JNkWYDtUn/zXw9pJ+x+RSh+EQpeZGEt71Y4laDl0PqYzSeR+AStahEF0Q=="}
)

###%fs ls /mnt/landing-files/SH/channels


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Source file path

# COMMAND ----------

###dbutils.widgets.removeAll()

####dbutils.widgets.text("job_name","job_channels")
####dbutils.widgets.text("landing_file_folder","channels")
####dbutils.widgets.text("landing_file_base_path","dbfs:/mnt/landing-files/SH/")
####dbutils.widgets.text("landing_file_name","channels06-12-2020-10-20-50.csv")
####dbutils.widgets.text("stage_database","stg_sales")
####dbutils.widgets.text("stage_table_name","stg_channels")

v_job_name = dbutils.widgets.get("job_name")
v_landing_file_folder = dbutils.widgets.get("landing_file_folder")
v_landing_file_name = dbutils.widgets.get("landing_file_name")
v_landing_file_path = dbutils.widgets.get("landing_file_base_path")+v_landing_file_folder+"/"+v_landing_file_name
v_stage_database = dbutils.widgets.get("stage_database")
v_stage_table_name = dbutils.widgets.get("stage_table_name")

print("Job Name: "+v_job_name)
print("Landing file folder: "+v_landing_file_folder)
print("Landing file path: "+v_landing_file_path)
print("Stage database name: "+v_stage_database)
print("Stage table name: "+v_stage_table_name)

# COMMAND ----------

from pyspark.sql.functions import *

v_schema = eval(v_landing_file_folder+"_schema()")
print(v_schema)

src_file_df = spark.read.option("mode","PREMISSIVE").option("header","true").csv(v_landing_file_path,schema=v_schema)
src_file_df = (src_file_df.withColumn("CREATED_DATE",lit(current_timestamp()))
        .withColumn("CREATED_BY",lit("ETLJob"))
        .withColumn("UPDATED_DATE",lit(current_timestamp()))
        .withColumn("UPDATED_BY",lit("ETLJob"))
        .withColumn("input_file",input_file_name())
        )

src_file_df.cache()
--display(src_file_df)

##src_file_df.unpersist()
### del(src_file_df)

src_file_df_good_data_records = src_file_df.filter("_corrupt_record is null").drop("_corrupt_record")
src_file_df_bad_data_records = src_file_df.filter("_corrupt_record is not null").select("_corrupt_record","input_file")

##display(src_file_df_bad_data_records)
##display(src_file_df_good_data_records)

src_file_df_good_data_records.write.format("delta").mode("overwrite").saveAsTable(v_stage_database+"."+v_stage_table_name)


# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC ---- Validation check
# MAGIC Select * from stg_sales.stg_channels;
# MAGIC
