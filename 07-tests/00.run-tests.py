# Databricks notebook source
# MAGIC %md
# MAGIC # 00.run-tests
# MAGIC
# MAGIC  - Reconciliation and integrity checks across silver and gold.
# MAGIC  - A failure raises, which fails the job task and triggers fail_batch,
# MAGIC  - so a broken batch never reaches reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC - Call the enrionment configuration helper

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

from pyspark.sql import functions as F

results = []

def check(name, actual, expected, note=""):
    ok = (actual == expected)
    results.append({"check": name, "expected": str(expected),
                    "actual": str(actual),
                    "status": "PASS" if ok else "FAIL", "note": note})
    print(f"{'PASS' if ok else 'FAIL'}  {name:52} expected {expected}, got {actual}")

def zero(name, query, note=""):
    check(name, spark.sql(query).count(), 0, note)

C = catalog_name

# COMMAND ----------

# MAGIC %md
# MAGIC - check if surrogate keys are unique

# COMMAND ----------

for table, key in [
    ("dim_customer", "customer_sk"),
    ("dim_customer_account", "bill_to_account_sk"),
    ("dim_product", "product_sk"),
    ("dim_campaign", "campaign_sk"),
    ("dim_date", "date_id"),
    ("fact_sales_order", "order_sk"),
    ("fact_sales_order_lines", "order_line_sk"),
    ("fact_shipment", "shipment_sk"),
]:
    zero(f"{table}.{key} unique", f"""
        SELECT {key} FROM {C}.{gold_schema}.{table}
        GROUP BY {key} HAVING COUNT(*) > 1
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC - check the SCD Type 2 integrity

# COMMAND ----------

zero("one current row per customer", f"""
    SELECT customer_id FROM {C}.{silver_schema}.cust_master
    WHERE is_current = true
    GROUP BY customer_id HAVING COUNT(*) > 1
""")

zero("no overlapping version periods", f"""
    SELECT a.customer_id
    FROM {C}.{silver_schema}.cust_master a
    JOIN {C}.{silver_schema}.cust_master b
           ON  a.customer_id = b.customer_id
           AND a.valid_from  < b.valid_from
    WHERE  COALESCE(a.valid_to, DATE'2999-12-31') > b.valid_from
""")

zero("closed versions have an end date", f"""
    SELECT customer_id FROM {C}.{silver_schema}.cust_master
    WHERE is_current = false AND valid_to IS NULL
""")

zero("current versions have no end date", f"""
    SELECT customer_id FROM {C}.{silver_schema}.cust_master
    WHERE is_current = true AND valid_to IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC - check if fact tables match silver's

# COMMAND ----------

for gold_table, silver_table in [
    ("fact_sales_order",       "sales_order"),
    ("fact_sales_order_lines", "sales_order_lines"),
    ("fact_shipment",          "shipment"),
]:
    g = spark.table(f"{C}.{gold_schema}.{gold_table}").count()
    s = spark.table(f"{C}.{silver_schema}.{silver_table}").count()
    check(f"{gold_table} row count matches silver", g, s)


zero("fact_sales_order -> dim_customer", f"""
    SELECT f.order_sk FROM {C}.{gold_schema}.fact_sales_order f
    LEFT JOIN {C}.{gold_schema}.dim_customer d ON f.customer_sk = d.customer_sk
    WHERE f.customer_sk IS NOT NULL AND d.customer_sk IS NULL
""")

zero("fact_sales_order -> dim_customer_account", f"""
    SELECT f.order_sk FROM {C}.{gold_schema}.fact_sales_order f
    LEFT JOIN {C}.{gold_schema}.dim_customer_account d
      ON f.bill_to_account_sk = d.bill_to_account_sk
    WHERE f.bill_to_account_sk IS NOT NULL AND d.bill_to_account_sk IS NULL
""")

zero("fact_sales_order_lines -> dim_product", f"""
    SELECT l.order_line_sk FROM {C}.{gold_schema}.fact_sales_order_lines l
    LEFT JOIN {C}.{gold_schema}.dim_product p ON l.product_sk = p.product_sk
    WHERE l.product_sk IS NOT NULL AND p.product_sk IS NULL
""")

zero("fact_sales_order_lines -> fact_sales_order", f"""
    SELECT l.order_line_sk FROM {C}.{gold_schema}.fact_sales_order_lines l
    LEFT JOIN {C}.{gold_schema}.fact_sales_order o ON l.order_sk = o.order_sk
    WHERE l.order_sk IS NOT NULL AND o.order_sk IS NULL
""")

zero("fact_shipment -> fact_sales_order", f"""
    SELECT s.shipment_sk FROM {C}.{gold_schema}.fact_shipment s
    LEFT JOIN {C}.{gold_schema}.fact_sales_order o ON s.order_sk = o.order_sk
    WHERE s.order_sk IS NOT NULL AND o.order_sk IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC - resolving date keys

# COMMAND ----------

for table, col in [
    ("fact_sales_order", "order_date_id"),
    ("fact_sales_order", "invoice_date_id"),
    ("fact_sales_order", "payment_date_id"),
    ("fact_sales_order_lines", "order_date_id"),
    ("fact_shipment", "ship_date_id"),
    ("fact_shipment", "delivery_date_id"),
]:
    zero(f"{table}.{col} -> dim_date", f"""
        SELECT f.{col} FROM {C}.{gold_schema}.{table} f
        LEFT JOIN {C}.{gold_schema}.dim_date d ON f.{col} = d.date_id
        WHERE f.{col} IS NOT NULL AND d.date_id IS NULL
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # results and raise

# COMMAND ----------

display(spark.createDataFrame(results))

failed = [r for r in results if r["status"] == "FAIL"]
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")

if failed:
    raise Exception(f"{len(failed)} check(s) failed: "
                    + ", ".join(r["check"] for r in failed))