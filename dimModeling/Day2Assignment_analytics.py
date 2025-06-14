# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, hash as hashFcn, upper, year, min as minValue, max as maxValue, sum as sparkSum, date_format, year, month, quarter, when, regexp_replace) 

# COMMAND ----------

spark = SparkSession.builder.appName("Day2").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Inner Joins

# COMMAND ----------

df_fact = spark.read.format("delta").load("/FileStore/tables/FACT_sales")

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_dim_region = spark.read.format("delta").load("/FileStore/tables/DIM_Region")

# COMMAND ----------

df_fact2 = df_fact.join(df_dim_region, df_dim_region.RegionKey == df_fact.DIM_RegionKey, "inner")

# COMMAND ----------

display(df_fact2)

# COMMAND ----------

df_dim_product_category = spark.read.format("delta").load("/FileStore/tables/DIM_Category")

df_fact3 = df_fact.join(df_dim_product_category, df_dim_product_category.CategoryKey == df_fact.DIM_CategoryKey, "inner")

# COMMAND ----------

display(df_fact3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analytics

# COMMAND ----------

files = dbutils.fs.ls("FileStore/tables")
for file in files:
    print(file.path)

# COMMAND ----------

# Total Sales Made By Each SalesRep
df_sales_rep = spark.read.format("delta").load("/FileStore/tables/DIM_sales_rep")

df_sales_by_customer = df_fact.join(df_sales_rep, df_sales_rep.SalesRepKey == df_fact.DIM_SalesRepKey, "inner").groupBy("Name").agg(sparkSum(col("UnitsSold")).alias("TotalUnitSold"),sparkSum(col("Revenue")).alias("TotalRevenue")).select(["Name","TotalUnitSold","TotalRevenue"])

display(df_sales_by_customer)


# COMMAND ----------

# Total Sales By Store Type in each year
df_store_type = spark.read.format("delta").load("/FileStore/tables/DIM_store_type")
df_date = spark.read.format("delta").load("/FileStore/tables/DIM_date")

df_sales_by_store_type = df_fact.join(df_store_type, df_store_type.StoreTypeKey == df_fact.DIM_StoreTypeKey, "inner") \
    .join(df_date, df_date.DateKey == df_fact.SalesDateKey, "inner") \
    .groupBy("Name","Year") \
    .agg(sparkSum(col("UnitsSold")).alias("TotalUnitSold"), sparkSum(col("Revenue")).alias("TotalRevenue")) \
    .select([col("Name").alias("StoreType"),"Year","TotalUnitSold","TotalRevenue"])

display(df_sales_by_store_type)


# COMMAND ----------

# Sales by saleschannel where product subcategory is Mobile or vegetable

df_saleschannel = spark.read.format("delta").load("/FileStore/tables/DIM_sales_channel")
df_subcategory = spark.read.format("delta").load("/FileStore/tables/DIM_SubCategory")

df_sales_by_saleschannel_subcat_mobile = df_fact.join(df_saleschannel.alias("storeType"), df_saleschannel.SalesChannelKey == df_fact.DIM_SalesChannelKey, "inner") \
    .join(df_subcategory.alias("subCategory"), df_subcategory.SubCategoryKey == df_fact.DIM_SubCategoryKey, "inner") \
    .groupBy(col("storeType.Name"),col("subCategory.Title")) \
    .agg(sparkSum(col("UnitsSold")).alias("TotalUnitSold"), sparkSum(col("Revenue")).alias("TotalRevenue")) \
    .filter((col("subCategory.Title") == "Mobile") | (col("subCategory.Title") == "Vegetables")) \
    .select([col("storeType.Name").alias("StoreType"),col("subCategory.Title").alias("SubCategory"),"TotalUnitSold","TotalRevenue"])

display(df_sales_by_saleschannel_subcat_mobile)