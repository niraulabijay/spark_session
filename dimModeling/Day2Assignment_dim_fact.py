# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, hash as hashFcn, upper, year, min as minValue, max as maxValue, date_format, year, month, quarter, when, regexp_replace) 
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType)
from datetime import datetime, timedelta

# COMMAND ----------

spark = SparkSession.builder.appName("Day2").getOrCreate()

# COMMAND ----------

df_sales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales-2.csv")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Dimension Region 

# COMMAND ----------

df_region = df_sales.select([
    col("Region")
]).distinct()

# COMMAND ----------

display(df_region)

# COMMAND ----------

df_regionAll = df_region.withColumn("RegionKey",hashFcn(upper(col("Region"))).cast("bigint"))

# COMMAND ----------

# add default N/A row for unknown/null values
df_na_region = spark.createDataFrame([
    ("N/A", -1)
],["Region","RegionKey"])
df_DIM_region = df_regionAll.unionAll(df_na_region)

# COMMAND ----------

# save region dim as delta table
df_DIM_region.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Region")

# COMMAND ----------

# read data from delta to see if it works
delta_dim_region = spark.read.format("delta").load("/FileStore/tables/DIM_Region")

# COMMAND ----------

display(delta_dim_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Dimension ProductCategory

# COMMAND ----------

df_category = df_sales.select([
    col("ProductCategory").alias("Title")
]).distinct()

# COMMAND ----------

df_category2 = df_category.withColumn("CategoryKey", hashFcn(upper(col("Title"))).cast("bigint"))

# COMMAND ----------

#handle n/a
df_cat_na = spark.createDataFrame([
    ("N/A",-1)
],["Title","CategoryKey"])
df_DIM_category = df_category2.unionAll(df_cat_na)

# COMMAND ----------

display(df_DIM_category)

# COMMAND ----------

df_DIM_category.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Category")

# COMMAND ----------

df_subcategory = df_sales.select([
    col("ProductSubCategory").alias("Title")
]).distinct()

# COMMAND ----------

df_subcategory2 = df_subcategory.withColumn("SubCategoryKey",hashFcn(upper(col("Title"))).cast("bigint"))

# COMMAND ----------

#handle unknown
df_na_subcat = spark.createDataFrame([
    ("N/A",-1)
],["Title","SubCategoryKey"])
df_DIM_subcategory = df_subcategory2.unionAll(df_na_subcat)

# COMMAND ----------

display(df_DIM_subcategory)

# COMMAND ----------

# write to delta table
df_DIM_subcategory.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_SubCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Dimension SalesChannel,CustomerSegment,SalesRep,StoreType

# COMMAND ----------

df_sales_channel = df_sales.select([
    col("SalesChannel").alias("Name")
]).distinct()

df_customer_segment = df_sales.select([
    col("CustomerSegment").alias("Segment")
]).distinct()

df_sales_rep = df_sales.select([
    col("SalesRep").alias("Name")
]).distinct()

df_store_type = df_sales.select([
    col("StoreType").alias("Name")
]).distinct()

# COMMAND ----------

df_sales_channel2 = df_sales_channel.withColumn("SalesChannelKey",hashFcn(upper(col("Name"))).cast("bigint"))

df_customer_segment2 = df_customer_segment.withColumn("CustomerSegmentKey",hashFcn(upper(col("Segment"))).cast("bigint"))

df_sales_rep2 = df_sales_rep.withColumn("SalesRepKey",hashFcn(upper(col("Name"))).cast("bigint"))

df_store_type2 = df_store_type.withColumn("StoreTypeKey",hashFcn(upper(col("Name"))).cast("bigint"))

# COMMAND ----------

df_sales_channel_na = spark.createDataFrame([
    ("N/A",-1)
],["Name","SalesChannelKey"])
df_DIM_sales_channel = df_sales_channel2.unionAll(df_sales_channel_na)

df_customer_segment_na = spark.createDataFrame([
    ("N/A",-1)
],["Segment","CsutomerSegmentKey"])
df_DIM_customer_segment = df_customer_segment2.unionAll(df_customer_segment_na)

df_sales_rep_na = spark.createDataFrame([
    ("N/A",-1)
],["Name","SalesRepKey"])
df_DIM_sales_rep = df_sales_rep2.unionAll(df_sales_rep_na)

df_store_type_na = spark.createDataFrame([
    ("N/A",-1)
],["Name","StoreTypeKey"])
df_DIM_store_type = df_store_type2.unionAll(df_store_type_na)

# COMMAND ----------

display(df_DIM_sales_channel)
display(df_DIM_customer_segment)
display(df_DIM_sales_rep)
display(df_DIM_store_type)

# COMMAND ----------

df_DIM_sales_channel.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_sales_channel")

df_DIM_customer_segment.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_customer_segment")

df_DIM_sales_rep.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_sales_rep")

df_DIM_store_type.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_store_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Date Dimension

# COMMAND ----------

# extract unique year from fact sales
df_unique_years = df_sales.select([
    year(col('SalesDate')).alias('Year').cast("int")
]).distinct()
#display(unique_years)

# COMMAND ----------

# Get min and max years
aggYear = df_unique_years.agg(
   minValue(col("Year")).alias("MinYear"),
   maxValue(col("Year")).alias("MaxYear"), 
)
minYear = aggYear.collect()[0][0]
maxYear = aggYear.collect()[0][1]


# COMMAND ----------

date_schema = StructType([
    StructField("DateKey", LongType(), True),
    StructField("Date", DateType(), True),
])

# had to use datetime library for date manipulation

start_date = datetime(minYear, 1, 1) 
end_date = datetime(maxYear, 12, 31) 

date_rows = []
current_date = start_date
while current_date <= end_date:
    date_key = int(current_date.strftime('%Y%m%d'))
    date_rows.append((date_key, current_date.date())) 
    current_date += timedelta(days=1)

df_date = spark.createDataFrame(date_rows, date_schema)

df_DIM_date = df_date.withColumn("MonthName", date_format(col("Date"),"MMMM")) \
                  .withColumn("Year", year(col("Date"))) \
                  .withColumn("Quarter", quarter(col("Date"))) \
                  .withColumn("semester", when(month(col("Date")) >= 8, "Fall").when(month(col("Date")) <= 5, "Spring").otherwise("Summer"))

# COMMAND ----------

df_DIM_date.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Fact Table With DIM Keys

# COMMAND ----------

df_fact2 = df_sales.withColumn("DIM_RegionKey", when(col("Region").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("Region")))).cast("bigint")) \
                  .withColumn("DIM_CategoryKey", when(col("ProductCategory").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("ProductCategory")))).cast("bigint")) \
                  .withColumn("DIM_SubCategoryKey", when(col("ProductSubCategory").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("ProductSubCategory")))).cast("bigint")) \
                  .withColumn("DIM_SalesChannelKey", when(col("SalesChannel").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("SalesChannel")))).cast("bigint")) \
                  .withColumn("DIM_CustomerSegmentKey", when(col("CustomerSegment").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("CustomerSegment")))).cast("bigint")) \
                  .withColumn("DIM_SalesRepKey", when(col("SalesRep").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("SalesRep")))).cast("bigint")) \
                  .withColumn("DIM_StoreTypeKey", when(col("StoreType").isNull(), -1) \
                              .otherwise(hashFcn(upper(col("StoreType")))).cast("bigint")) \
                  .withColumn("SalesDateKey", when(col("SalesDate").isNull(), -1) \
                              .otherwise(regexp_replace(col("SalesDate"),"-","")).cast("bigint")) \
                              .drop(col("Region")) \
                              .drop(col("ProductCategory")) \
                              .drop(col("ProductSubCategory")) \
                              .drop(col("SalesChannel")) \
                              .drop(col("CustomerSegment")) \
                              .drop(col("SalesRep")) \
                              .drop(col("StoreType")) \
                              .drop(col("SalesDate")) 


# COMMAND ----------

display(df_fact2)

# COMMAND ----------

df_fact2.write.format("delta").mode("overwrite").save("/FileStore/tables/FACT_sales")