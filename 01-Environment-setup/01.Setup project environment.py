# Databricks notebook source
# MAGIC %md
# MAGIC # Set-up project environment for Kestrel data engineering project
# MAGIC - Create catalog Kestrel
# MAGIC - Create schema landing, bronze, silver and gold
# MAGIC - Create Volume files in the landing schema

# COMMAND ----------

# MAGIC %md
# MAGIC # Create catalog: Kestrel_data_eng_prj

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS kestrel_data_eng_prj
# MAGIC     COMMENT 'This catalog is used for the kestrel_data_eng_prj project';

# COMMAND ----------

# MAGIC %md
# MAGIC # Create schema landing, bronze, silver and gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kestrel_data_eng_prj.landing
# MAGIC     COMMENT 'This schema is used for the kestrel_data_eng_prj project landing';
# MAGIC CREATE SCHEMA IF NOT EXISTS kestrel_data_eng_prj.bronze
# MAGIC     COMMENT 'This schema is used for the kestrel_data_eng_prj project bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS kestrel_data_eng_prj.silver
# MAGIC     COMMENT 'This schema is used for the kestrel_data_eng_prj project silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS kestrel_data_eng_prj.gold
# MAGIC     COMMENT 'This schema is used for the kestrel_data_eng_prj project gold';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG kestrel_data_eng_prj;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Volume files in the landing schema

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS kestrel_data_eng_prj.landing.files
# MAGIC     COMMENT 'This volume is used for the kestrel_data_eng_prj project landing';

# COMMAND ----------

# MAGIC %fs ls /Volumes/kestrel_data_eng_prj/landing/files