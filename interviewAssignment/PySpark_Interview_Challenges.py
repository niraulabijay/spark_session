# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 1: Total Spend Per Customer
# MAGIC
# MAGIC Calculate total amount spent by each customer.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (col, sum as sparkSum)

# COMMAND ----------

data = [
    Row(customer_id=1, amount=250),
    Row(customer_id=2, amount=450),
    Row(customer_id=1, amount=100),
    Row(customer_id=3, amount=300),
    Row(customer_id=2, amount=150)
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

df_total_per_customer = df.groupBy("customer_id").agg(sparkSum(col("amount")).alias("TotalAmountSpent"))
display(df_total_per_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC Basically, I used the aggregate sum function to find total amount spent for each customer. I used import alias sparkSum in order to prevent collision with python's native sum() function.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 2: Highest Transaction Per Day
# MAGIC
# MAGIC Find the highest transaction amount for each day.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (col, sum as sparkSum, row_number)
from pyspark.sql.window import Window

data = [
    Row(date='2023-01-01', amount=100),
    Row(date='2023-01-01', amount=300),
    Row(date='2023-01-02', amount=150),
    Row(date='2023-01-02', amount=200)
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

windowSpec = Window.partitionBy(df.date).orderBy(df.amount.desc())
df_ranked = df.withColumn("dailyRank",row_number().over(windowSpec))
df_highest = df_ranked.filter(col("dailyRank") == 1) \
    .select([col('date'), col('amount')])
df_highest.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, since we have to find highest transaction per day, the first step would be to re-arrange amount into descending order for each day. This can be achieved with window function rank_number with partition over the date column. After this, we just select each the rows having rank of 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 3: Fill Missing Cities With Default
# MAGIC
# MAGIC Replace null city values with 'Unknown'.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row

data = [
    Row(customer_id=1, city='Dallas'),
    Row(customer_id=2, city=None),
    Row(customer_id=3, city='Austin'),
    Row(customer_id=4, city=None)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df_filled = df.fillna({"city":"Unknown"})
df_filled.show()

# COMMAND ----------

# MAGIC %md
# MAGIC I used fillna() to replace any null values in city column with "Unknown" value

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 4: Compute Running Total by Customer
# MAGIC
# MAGIC Use a window function to compute cumulative sum of purchases per customer.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (lag)

data = [
    Row(customer_id=1, date='2023-01-01', amount=100),
    Row(customer_id=1, date='2023-01-02', amount=200),
    Row(customer_id=1, date='2023-01-03', amount=300),
    Row(customer_id=2, date='2023-01-01', amount=300),
    Row(customer_id=2, date='2023-01-02', amount=400),
    Row(customer_id=2, date='2023-01-04', amount=500)
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

windowSpec = Window.partitionBy(col("customer_id")).orderBy(col("date"))
df_check_lag = df.withColumn("previousVal",lag(col('amount'),1,0).over(windowSpec))
display(df_check_lag)

# COMMAND ----------

#df_running_total = df_check_lag.withColumn("Running Total", col('amount')+col('previousVal'))
df_running_total = df_check_lag.withColumn("Running Total", sparkSum(col('amount')) \
    .over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
display(df_running_total)

# COMMAND ----------

# MAGIC %md
# MAGIC This was a new challenge in terms of window function. My first thought was to use lag function to get previous value and calculate sum on the go, but then i realised this would only give me sum of 2 rows max. Then on searching pyspark docs (https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html), Window function has attribute rowsBetween() where i can specify range from unboundedPreceding (which is a very large negative number, so i presume if goes back to rows before current row context) and currentRow, thus giving the running total. Another similar attribute rangeBetween() was also there, but it seems to work on the column value of the row context and not physical position of the row. So, using the sum over the window specification provided the running total. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 5: Average Sales Per Product
# MAGIC
# MAGIC Find average amount per product.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (avg as sparkAvg)

data = [
    Row(product='A', amount=100),
    Row(product='B', amount=200),
    Row(product='A', amount=300),
    Row(product='B', amount=400)
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

data_avg_product = df.groupBy(col("product")) \
    .agg(sparkAvg(col("amount")).alias("average_amount"))
data_avg_product.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Since the goal is to find average per product, first we group the data by product name and apply the aggregate function avg() on the amount column. Again, sparkAvg is used as alias to avg to avoid conflicts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 6: Extract Year From Date
# MAGIC
# MAGIC Add a column to extract year from given date.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (year as sparkYear)

data = [
    Row(customer='John', transaction_date='2022-11-01'),
    Row(customer='Alice', transaction_date='2023-01-01')
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

df_extract = df.withColumn("Year", sparkYear(col("transaction_date")))
df_extract.show()

# COMMAND ----------

# MAGIC %md
# MAGIC A simple year() function, like in sql, can extract year from the date column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 7: Join Product and Sales Data
# MAGIC
# MAGIC Join two DataFrames on product_id to get product names with amounts.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row

products = [
    Row(product_id=1, product_name='Phone'),
    Row(product_id=2, product_name='Tablet')
]
sales = [
    Row(product_id=1, amount=500),
    Row(product_id=2, amount=800),
    Row(product_id=1, amount=200)
]
df_products = spark.createDataFrame(products)
df_sales = spark.createDataFrame(sales)
df_products.show()
df_sales.show()


# COMMAND ----------

df_joined = df_products.join(df_sales, df_products.product_id == df_sales.product_id, how="inner") \
    .select([df_products['*'], df_sales.amount])
display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC I used the inner join to show product name and sales amount in a single table. Also, I used select function to select all columns of product table and just amount from sales in order to hide duplicate product_id column due to the resulting join.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 8: Split Tags Into Rows
# MAGIC
# MAGIC Given a list of comma-separated tags, explode them into individual rows.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import (split, explode)

data = [
    Row(id=1, tags='tech,news'),
    Row(id=2, tags='sports,music'),
    Row(id=3, tags='food')
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

df_split = df.select([col('id'), split(col('tags'),',').alias("split_tags")])
df_split.show()
print(df_split.count())

# COMMAND ----------

df_exploded = df_split.withColumn("individual_tag",explode(col("split_tags")))
#df_exploded.show()
df_final = df_exploded.drop(col("split_tags"))
df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC I searched for function to split the string separated by ",". This created a list of tags in the "split_tags" column but the records were still in the same row. So, based on instructions given, I again searched for "explode" function which was specifically used to generate new rows in dataframe based on array values in column provided in the argument.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 9: Top-N Records Per Group
# MAGIC
# MAGIC For each category, return top 2 records based on score.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row

data = [
    Row(category='A', name='x', score=80),
    Row(category='A', name='y', score=90),
    Row(category='A', name='z', score=70),
    Row(category='B', name='p', score=60),
    Row(category='B', name='q', score=85)
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

windowSpec = Window.partitionBy(col('category')).orderBy(col('score').desc())
df_ranked = df.withColumn("rankedScore", row_number().over(windowSpec))
df_ranked.show()

# COMMAND ----------

df_top2 = df_ranked.filter(col("rankedScore") <= 2).select([
    col("category"), col("name"), col("score")
])
df_top2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC This was similar to **Challenge 2** but instead of just getting highest(top 1), we need to get top 2 for each category. So, a ranking function (row_number()) follwed by filter for rank column <= 2 would give the top 2 records for each category

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 10: Null Safe Join
# MAGIC
# MAGIC Join two datasets where join key might have nulls, handle using null-safe join.
# MAGIC

# COMMAND ----------


from pyspark.sql import Row

data1 = [
    Row(id=1, name='John'),
    Row(id=None, name='Mike'),
    Row(id=2, name='Alice'),
    Row(id=None, name='Johnson'),
]
data2 = [
    Row(id=1, salary=5000),
    Row(id=None, salary=3000),
    Row(id=None, salary=2000),
    Row(id=None, salary=1500)
]
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)
df1.show()
df2.show()


# COMMAND ----------

df_joined_null_safe = df1.join(df2, df1.id.eqNullSafe(df2.id), how='inner')
df_joined_null_safe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC I searched and found the null safe join operator in pyspark which is eqNullSafe. Using this the null values are also included in the joined table. TO visualize more clearly, i added 1 more null row in both tables, and as expected, it joins null rows in combination (mxn). Also, it didnot matter which join (left, right, inner) i used, the _null rows_ were the same in count.