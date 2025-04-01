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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM realestate.transactions_old")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old1= df.select(
    col('transaction_id'),  # No cast needed, assuming it's already the correct type
    col('instance_date'),  # Casting to date type
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
display(Filtered_old1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old1.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old1.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_old2 = df.select(
    col('transaction_id'),  # No cast needed, assuming it's already the correct type
    to_date(col('instance_date'), 'yyyy-MM-dd').alias('instance_date'),  # Cast to date type with proper format
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
display(Filtered_old2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select('instance_date').distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = df.withColumn(
    'instance_date',
    to_date(col('instance_date'), 'dd-MM-yyyy')  # Correct date format for the input data
)
display(df_cleaned)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Filtered_olddc= df.select(
    col('transaction_id'),  # No cast needed, assuming it's already the correct type
    col('instance_date'),  # Casting to date type
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
display(Filtered_olddc)

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
