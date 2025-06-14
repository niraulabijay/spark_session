# Databricks notebook source
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType 
from datetime import date
from pyspark.sql.functions import( col, sum as sparkSum, upper, hash as hashForKey, when)

# COMMAND ----------

spark = SparkSession.builder.appName("FactTransactions").getOrCreate()

# COMMAND ----------


# Define schema
schema = StructType([
    StructField("TransactionID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("TransactionDate", DateType(), True)
])

# Sample data
data = [
    (1, "Alice", "East", 200.0, date(2023, 1, 1)),
    (2, "Bob", "West", 150.0, date(2023, 1, 2)),
    (3, "Alice", "East", 400.0, date(2023, 1, 5)),
    (4, "David", "South", 300.0, date(2023, 1, 6)),
    (5, "Eve", "North", 500.0, date(2023, 1, 7)),
    (6, "Bob", "West", 100.0, date(2023, 1, 10)),
    (7, "Frank", "East", 250.0, date(2023, 1, 12)),
    (8, "Alice", "East", 350.0, date(2023, 1, 15))
]

# COMMAND ----------

df_fact = spark.createDataFrame(data, schema)

# COMMAND ----------

df_fact.show()

# COMMAND ----------

df_analytics = df_fact.groupBy(
    col("CustomerName"),
    col("Region")
).agg(
   sparkSum(col("Amount")).alias("Sum_of_Amount") 
)

display(df_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM Region

# COMMAND ----------

dfRegion = df_fact.select([
    col("Region")
]).distinct()

# COMMAND ----------

display(dfRegion)

# COMMAND ----------

dfRegion2 = dfRegion.withColumn("DIM_RegionId", hash(upper(col("Region"))).cast("bigint"))

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["Region", "DIM_RegionId"])

dfDimFinal = dfRegion2.union(dfBase)

# COMMAND ----------

display(dfDimFinal)

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Region")

# COMMAND ----------

df_DIMRegion = spark.read.format("delta").load("/FileStore/tables/DIM_Region")

# COMMAND ----------

display(df_DIMRegion)

# COMMAND ----------

df_fact2 = df_fact.withColumn("DIM_RegionId", when(col("Region").isNull(), -1).otherwise(hash(upper(col("Region")))).cast("bigint")).drop(col("Region"))

# COMMAND ----------

display(df_fact2)

# COMMAND ----------

dfJoin = df_fact2.join(df_DIMRegion, df_DIMRegion.DIM_RegionId==df_fact2.DIM_RegionId, how ="inner")

# COMMAND ----------

display(dfJoin)