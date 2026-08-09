# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# basic transformation function
def trim_whitespaces(df):
    for c in df.columns:
        if df.schema[c].dataType == StringType():
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

# remove nulls from business keys
def remove_nulls(df, columns):
    for c in columns:
        df = df.filter(F.col(c).isNotNull())
    return df

# remove duplicates
def remove_duplicates(df, columns):
    for c in columns:
        df = df.dropDuplicates([c])
    return df


# COMMAND ----------

# write to silver function
def write_to_silver(
    input_df,
    target_table,
    merge_condition,
    columns_to_update
):# defines a function that accepts four arguments:
    """
    create the Delta table if it does not exists.
    otherwise merges the input dataframe into the target table.
    """
    final_df = input_df.withColumn("created_timestamp", F.current_timestamp()).withColumn("updated_timestamp", F.current_timestamp()) # adds two new columns to the input dataframe

    if not spark.catalog.tableExists(target_table): # checks if the target table exists in the spark catalog # the not inverts the result, so nested code is executed only if the table does not exists
        final_df.write.format('delta').mode('overwrite').saveAsTable(target_table) # this creates the table for the first time and specify the data should be seved in delta format
    else:
        delta_table = DeltaTable.forName(spark, target_table) # this line executes, if the condition above is false, meaning target_table already exists - loads the existing Delta table into a DeltaTable object
        update_map = {column: f"s.{column}" for column in columns_to_update} # creates a dictionary - this map is used to specify the columns to be updated in the merge, it creates a key-value pair
        update_map["updated_timestamp"] = "s.updated_timestamp" # this explicitly adds or updates the updated_timestam field in the update_map everyt time a row is updated.
        (
            delta_table.alias("t") #assigns t for target to the existing delta table
            .merge( # upsert logic
                final_df.alias("s"), # assigns s for source to the final df
                merge_condition
            )
            .whenMatchedUpdate(
                condition="s.batch_id >= t.batch_id", # additional condition. Allow to update if the batch id of the source is greater than or equal to the batch id of the target row. this prevents older data from overwriting newer data
                set=update_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )