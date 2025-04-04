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
# META       "default_lakehouse_workspace_id": "4eb54e95-eab8-42c4-8eab-decf34b17810",
# META       "known_lakehouses": [
# META         {
# META           "id": "34a54ecb-b102-40db-bf13-e723dcf463c2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # To do 
# ## Remove wadi Al safa 2

# CELL ********************

from pyspark.sql.functions import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Remove Unnecessary Rows and change the date 


# CELL ********************

df = spark.sql("SELECT * FROM realestate.bronze_transactions_old")
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date_cleaned = df.withColumn(
    'instance_date',
    to_date(col('instance_date'), 'dd-MM-yyyy')  # Correct date format for the input data
)
#display(df_date_cleaned)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old= df_date_cleaned.select(
    col('transaction_id'),  # No cast needed, assuming it's already the correct type
    col('instance_date').cast("date"),  # Casting to date type
    col('property_type_en'),
    col('property_usage_en'),
    col('reg_type_en'),
    col('area_name_en'),
    col('master_project_en'),
    col('nearest_landmark_en'),
    col('rooms_en'),
    col('procedure_area').cast('int'),  # Casting to integer
    col('actual_worth').cast('int'),  # Casting to integer
    col('meter_sale_price').cast('int'),  # Casting to integer
    col('rent_value').cast('int'),  # Casting to integer
    col('meter_rent_price').cast('int')  # Casting to integer
)
#display(Filtered_old)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check for other necessary column and infer those columns 
# #### Filtered_old is the final data frame 


# CELL ********************

change1= Filtered_old

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

change1.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(change1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

change1.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Infer the columns which has NULL values

# CELL ********************

# q= change1.filter(col('transaction_id').isNull())
# #display(q)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# q= change1.filter(col('instance_date').isNull())
# #display(q)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# q= change1.filter(col('property_type_en').isNull())
# #display(q)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col8= change1.filter(col('nearest_landmark_en').isNull())
#display(col8)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

asd = col8.groupBy('area_name_en','nearest_landmark_en')\
          .agg(count('transaction_id').alias('transaction_id_count')) \
          .select(col('area_name_en'), col('nearest_landmark_en'), col('transaction_id_count'))

#display(asd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test1 = Filtered_old.filter((col('area_name_en') == col('nearest_landmark_en')) & (col('nearest_landmark_en') != "Burj Khalifa") & (col('nearest_landmark_en') != "Dubai International Airport"))\
                    .select(col('area_name_en'), col('nearest_landmark_en'))
#display(test1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col8 = change1.withColumn(
    'nearest_landmark_en',  # New column name (or the same column name)
    when(col('nearest_landmark_en').isNull(), col('area_name_en'))  # Condition and value from another column
    .otherwise(col('nearest_landmark_en'))  # Retain original value of 'column_1' if condition is not met
)
#display(col8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col8 = col8.withColumn(
    'area_name_en',  # New column name (or the same column name)
    when(col('area_name_en').isNull(), col('nearest_landmark_en'))  # Condition and value from another column
    .otherwise(col('area_name_en'))  # Retain original value of 'column_1' if condition is not met
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col8 = col8.withColumn(
    'master_project_en',  # Column name
    when(col('master_project_en').isNull(), 'NA')  # Replace NULL with 'NA'
    .otherwise(col('master_project_en'))  # Retain original value if not NULL
)

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

# CELL ********************

col8.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # check


# CELL ********************

Filtered_old= col8

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use `agg()` and `isNull()` to count null values in each column directly
null_counts = Filtered_old.agg(
    sum(when(col('property_type_en').isNull(), 1).otherwise(0)).alias('property_type_en_nulls'),
    sum(when(col('property_usage_en').isNull(), 1).otherwise(0)).alias('property_usage_en_nulls'),
    sum(when(col('reg_type_en').isNull(), 1).otherwise(0)).alias('reg_type_en_nulls'),
    sum(when(col('area_name_en').isNull(), 1).otherwise(0)).alias('area_name_en_nulls'),
    sum(when(col('master_project_en').isNull(), 1).otherwise(0)).alias('master_project_en_nulls'),
    sum(when(col('nearest_landmark_en').isNull(), 1).otherwise(0)).alias('nearest_landmark_en_nulls')
)

# Show the result
display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q= change1.filter(col('actual_worth').isNotNull())
p = change1.filter(col('actual_worth').isNull())
print(q.count(),p.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

qwert = change1.withColumn(
    'actual_worth', 
    when(col('actual_worth').isNull(), col('procedure_area') * col('meter_sale_price'))
     .otherwise(col('actual_worth'))
)
display(qwert)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q1= qwert.filter(col('actual_worth').isNull())

display(q1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old = qwert

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df123f=Filtered_old.filter(col('area_name_en').isNull())
display(df123f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df123f2=Filtered_old.filter(col('actual_worth').isNull())
display(df123f2)

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

# MARKDOWN ********************

# # Checkpoint

# CELL ********************

q= change1.filter(col('meter_sale_price').isNull())
#display(q)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q= change1.filter(col('rent_value').isNull())
#display(q)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q= change1.filter(col('meter_rent_price').isNull())
#display(q)






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # clean the date with wrong entry


# CELL ********************

Filtered_old= Filtered_old.filter(Filtered_old.instance_date >= '1582-10-15')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old = Filtered_old.filter(col('area_name_en')!='Wadi Al Safa 2')
#display(Filtered_old)

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

# CELL ********************

Filtered_old.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Writing table to Silver table

# CELL ********************

Filtered_old.write.mode("overwrite").saveAsTable("silver_transaction_old")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Quality checks

# CELL ********************

display(Filtered_old)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

check1= Filtered_old.select()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use `agg()` and `isNull()` to count null values in each column directly
null_counts = Filtered_old.agg(
    sum(when(col('property_type_en').isNull(), 1).otherwise(0)).alias('property_type_en_nulls'),
    sum(when(col('property_usage_en').isNull(), 1).otherwise(0)).alias('property_usage_en_nulls'),
    sum(when(col('reg_type_en').isNull(), 1).otherwise(0)).alias('reg_type_en_nulls'),
    sum(when(col('area_name_en').isNull(), 1).otherwise(0)).alias('area_name_en_nulls'),
    sum(when(col('master_project_en').isNull(), 1).otherwise(0)).alias('master_project_en_nulls'),
    sum(when(col('nearest_landmark_en').isNull(), 1).otherwise(0)).alias('nearest_landmark_en_nulls')
)

# Show the result
display(null_counts)


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
