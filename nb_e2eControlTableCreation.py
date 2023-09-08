# Databricks notebook source
# MAGIC %sql
# MAGIC Create database mddb;
# MAGIC
# MAGIC %sql 
# MAGIC drop table mddb.control_table;
# MAGIC Create table  mddb.control_table(
# MAGIC         job_id int,
# MAGIC         job_name string,
# MAGIC         src_file_path string,
# MAGIC         src_file_folder string,
# MAGIC         landing_file_base_path string,
# MAGIC         landing_file_folder string,
# MAGIC         stage_database string,
# MAGIC         stage_table_name string
# MAGIC         )
# MAGIC
# MAGIC --%sql describe formatted mddb.control_table
# MAGIC
# MAGIC %sql
# MAGIC Insert into mddb.control_table 
# MAGIC Values(1,"job_countries","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","countries","/mnt/landing-files/SH","countries","stg_sales","countries")
# MAGIC ,(2,"job_channels","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","channels","/mnt/landing-files/SH","channels","stg_sales","channels")
# MAGIC ,(3,"job_costs","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","costs","/mnt/landing-files/SH","costs","stg_sales","costs")
# MAGIC ,(4,"job_customers","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","customers","/mnt/landing-files/SH","customers","stg_sales","customers")
# MAGIC ,(5,"job_product","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","product","/mnt/landing-files/SH","product","stg_sales","product")
# MAGIC ,(6,"job_promotions","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","promotions","/mnt/landing-files/SH","promotions","stg_sales","promotions")
# MAGIC ,(7,"job_sales","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","sales","/mnt/landing-files/SH","sales","stg_sales","sales")
# MAGIC ,(8,"job_times","C:\\Users\\Mahipal\\Desktop\\Anil\\training material\\classes\\sales_data_files\\SH","times","/mnt/landing-files/SH","times","stg_sales","times")	
# MAGIC
# MAGIC
# MAGIC
# MAGIC %sql Select * from mddb.control_table
# MAGIC
# MAGIC %fs ls dbfs:/user/hive/warehouse/mddb.db/
