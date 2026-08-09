# Databricks notebook source
catalog_name = 'kestrel_data_eng_prj'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
control_schema = 'control'
invoice_line_path = 'invoice_lines'
invoice = 'invoice'
payment = 'payment'
sales_order_lines = 'sales_order_lines'
sales_order = 'sales_order'
shipment = 'shipments'



# COMMAND ----------

landing_folder_path = '/Volumes/kestrel_data_eng_prj/landing/files'
