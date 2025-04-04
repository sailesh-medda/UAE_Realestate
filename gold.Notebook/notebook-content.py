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

# CELL ********************

from pyspark.sql.functions import *


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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold

# CELL ********************

silver_transaction_old = spark.sql("SELECT * FROM realestate.silver_transaction_old")
display(silver_transaction_old)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = silver_transaction_old.groupBy(col('reg_type_en'))\
                 .agg(count('reg_type_en').alias('hello'))\
                 .select(col('reg_type_en'),col('hello'))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_transaction_old.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimdf = silver_transaction_old.select(
    'property_type_en',
    'property_usage_en',
    'reg_type_en',
    'area_name_en',
    'master_project_en',
    'nearest_landmark_en'
)
display(dimdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_type = silver_transaction_old.select(
    'property_type_en',
    'rooms_en',
    'property_usage_en',
    'reg_type_en'
).distinct()
display(dim_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_type = dim_type.withColumn(
    'dim_type_key', monotonically_increasing_id()
)
display(dim_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_type_gold = dim_type

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_type=dim_type.select(col('dim_type_key'),col('property_type_en'),col('rooms_en'),col('property_usage_en'),col('reg_type_en'))
display(dim_type)

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

test_dim_area = silver_transaction_old.select(
    'area_name_en',
    'master_project_en',
    'nearest_landmark_en'
).distinct()
display(test_dim_area)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test2_dim_area = test_dim_area.withColumn(
    'dim_area_key', monotonically_increasing_id()
)
display(test2_dim_area)

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

dim_area = test2_dim_area.select(
    'dim_area_key', 'area_name_en', 'master_project_en', 'nearest_landmark_en'
)
display(dim_area)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_area_gold = dim_area

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_areadim = silver_transaction_old.join(
    dim_area,
    (silver_transaction_old.area_name_en == dim_area.area_name_en) & 
    (silver_transaction_old.master_project_en == dim_area.master_project_en) &
    (silver_transaction_old.nearest_landmark_en == dim_area.nearest_landmark_en),
    'left'
)
display(join_areadim)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

l =join_areadim.filter(col('dim_area_key').isNull())
display(l)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_areadim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Check for duplicates in dim_type
duplicates_in_dim_type = dim_type.groupBy(
    'rooms_en', 
    'property_usage_en', 
    'reg_type_en'
).count().filter(F.col('count') > 1)

display(duplicates_in_dim_type)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_areadim = join_areadim.withColumnRenamed('area_name_en', 'join_area_name_en') \
                           .withColumnRenamed('master_project_en', 'join_master_project_en') \
                           .withColumnRenamed('nearest_landmark_en', 'join_nearest_landmark_en')

# Now, perform the join:
join_typedim = join_areadim.join(
    dim_type,
    (silver_transaction_old.rooms_en == dim_type.rooms_en) & 
    (silver_transaction_old.property_usage_en == dim_type.property_usage_en) &
    (silver_transaction_old.reg_type_en == dim_type.reg_type_en)&
    (silver_transaction_old.property_type_en == dim_type.property_type_en),
    'inner'
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_typedim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_areadim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_typedim = join_typedim.withColumnRenamed('rooms_en', 'join_rooms_en') \
                           .withColumnRenamed('property_usage_en', 'join_property_usage_en') \
                           .withColumnRenamed('reg_type_en', 'join_reg_type_en')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


join_typedim.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

combined_dim = join_typedim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(combined_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the fact table by excluding the dimension columns
fact_trans = join_typedim.select(
    'transaction_id',
    'instance_date',
    'procedure_area',
    'actual_worth',
    'meter_sale_price',
    'rent_value',
    'meter_rent_price',
    'dim_type_key',
    'dim_area_key'

)

# Display the fact table
display(fact_trans)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

k = fact_trans.filter(col('transaction_id').isNull())
display(k)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_transaction_old.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get distinct rows based on the transaction_id column
df = silver_transaction_old.select('transaction_id').distinct()

# Show the result
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get distinct rows based on the transaction_id column
df = fact_trans.select('transaction_id').distinct()

# Show the result
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_trans.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_trans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_trans.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_trans.count()

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

# # Writing to gold table

# CELL ********************

fact_trans.write.mode("overwrite").saveAsTable("gold_fact_transaction_old")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_type_gold.write.mode("overwrite").saveAsTable("gold_dim_type_transaction_old")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_area_gold.write.mode("overwrite").saveAsTable("gold_dim_area_transaction_old")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_trans.count()

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
