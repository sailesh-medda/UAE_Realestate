# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "34a54ecb-b102-40db-bf13-e723dcf463c2",
# META       "default_lakehouse_name": "realestate",
# META       "default_lakehouse_workspace_id": "4eb54e95-eab8-42c4-8eab-decf34b17810"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df90= spark.sql("SELECT * FROM realestate.transactions_old")
k = df90.count()
print(k)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM realestate.transactions_old LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df1 = df.agg(count("area_name_en").alias("countofprop"))\
        .groupBy("area_name_en")

df1.display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Group by 'area_name_en' first, then count occurrences
df1 = df90.groupBy("area_name_en")\
        .agg(count("area_name_en").alias("countofprop"))\
        .agg(sum("actual_worth").alias("amount"))\
        .orderBy("countofprop", ascending=False) 

# Now you can display the result
display(df1) # or df1.display() if you're in Databricks or similar environments



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

k = df.count()  # Get the number of rows
print(k)  # Print the row count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Get the minimum and maximum of the 'instance_date' column
min_date = df90.agg(min("instance_date")).collect()[0][0]
max_date = df90.agg(max("instance_date")).collect()[0][0]

# Print the results
print("Min Date:", min_date)
print("Max Date:", max_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Cast 'actual_worth' to float and then perform aggregation
df1 = df90.groupBy("area_name_en")\
           .agg(
               count("area_name_en").alias("countofprop"),
               sum(col("actual_worth").cast("float")).alias("amount")
           )\
           .orderBy("countofprop", ascending=False)

# Display the result (for Databricks or similar environments)
display(df1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter rows where 'area_name_en' is 'Remah'
remah_row = df90.filter(df90.area_name_en == 'Remah')

# Display the result (use show() for non-Databricks environments)
display(remah_row)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Group by 'area_name_en' first, then count occurrences
df12 = df90.groupBy("area_name_en")\
        .agg(count("area_name_en").alias("countofprop"))\
        .orderBy("countofprop", ascending=False) 

# Now you can display the result
display(df12) # or df1.display() if you're in Databricks or similar environments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
