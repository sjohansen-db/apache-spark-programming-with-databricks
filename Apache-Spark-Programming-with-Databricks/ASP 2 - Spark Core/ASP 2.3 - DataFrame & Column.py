# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC ##### Objectives
# MAGIC 1. Construct columns
# MAGIC 1. Subset columns
# MAGIC 1. Add or replace columns
# MAGIC 1. Subset rows
# MAGIC 1. Sort rows
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, operators

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md Let's use the BedBricks events dataset.

# COMMAND ----------

events_df = spark.read.format("delta").load(events_path)
display(events_df)

# COMMAND ----------

print(events_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC val eventsPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/events/events.delta"
# MAGIC val eventsDf = spark.read.format("delta").load(eventsPath)
# MAGIC display(eventsDf)

# COMMAND ----------

# MAGIC %md ## Column Expressions
# MAGIC 
# MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC 
# MAGIC Construct a new Column based on existing columns in a DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.Column
# MAGIC 
# MAGIC println(eventsDf.col("device"))
# MAGIC println(eventsDf("device"))
# MAGIC println(col("device"))

# COMMAND ----------

# MAGIC %md
# MAGIC Scala supports an additional syntax for creating a new Column based on existing columns in a DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# MAGIC %scala
# MAGIC eventsDf.select('device)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

# MAGIC %md Create complex expressions with existing columns, operators, and methods.

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# MAGIC %scala
# MAGIC col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
# MAGIC col("event_timestamp").desc
# MAGIC (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# MAGIC %md
# MAGIC Here's an example of using these column expressions in the context of a DataFrame

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val revDf = eventsDf
# MAGIC   .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull)
# MAGIC   .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
# MAGIC   .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
# MAGIC   .sort(col("avg_purchase_revenue").desc)
# MAGIC 
# MAGIC display(revDf)

# COMMAND ----------

# MAGIC %md ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# MAGIC %md ### Subset columns
# MAGIC Use DataFrame transformations to subset columns

# COMMAND ----------

# MAGIC %md #### **`select()`**
# MAGIC Selects a list of columns or column based expressions

# COMMAND ----------

devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val devicesDf = eventsDf.select("user_id", "device")
# MAGIC display(devicesDf)

# COMMAND ----------

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val locationsDf = eventsDf.select(
# MAGIC   col("user_id"),
# MAGIC   col("geo.city").alias("city"),
# MAGIC   col("geo.state").as("state")
# MAGIC )
# MAGIC 
# MAGIC display(locationsDf)

# COMMAND ----------

# MAGIC %md #### **`selectExpr()`**
# MAGIC Selects a list of SQL expressions

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val appleDf = eventsDf
# MAGIC   .selectExpr(
# MAGIC     "user_id",
# MAGIC     "device in ('macOS', 'iOS') as apple_user"
# MAGIC   )
# MAGIC 
# MAGIC display(appleDf)

# COMMAND ----------

# MAGIC %md #### **`drop()`**
# MAGIC Returns a new DataFrame after dropping the given column, specified as a string or Column object
# MAGIC 
# MAGIC Use strings to specify multiple columns

# COMMAND ----------

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val anonymousDf = eventsDf.drop("user_id", "geo", "device")
# MAGIC display(anonymousDf)

# COMMAND ----------

no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)


# COMMAND ----------

# MAGIC %scala
# MAGIC val noSalesDf = eventsDf.drop(col("ecommerce"))
# MAGIC 
# MAGIC display(noSalesDf)

# COMMAND ----------

# MAGIC %md ### Add or replace columns
# MAGIC Use DataFrame transformations to add or replace columns

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **`withColumn()`**
# MAGIC Returns a new DataFrame by adding a column or replacing an existing column that has the same name.

# COMMAND ----------

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

mobile_df.groupBy("mobile").count().show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val mobileDf = eventsDf.withColumn("mobile", col("device").isin("iOS", "Android"))
# MAGIC 
# MAGIC display(mobileDf)

# COMMAND ----------

# MAGIC %scala
# MAGIC mobileDf.groupBy("mobile").count.show

# COMMAND ----------

purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_quantity_df.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC val purchaseQuantityDf = eventsDf.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
# MAGIC 
# MAGIC purchaseQuantityDf.select("purchase_quantity").printSchema

# COMMAND ----------

# MAGIC %md #### **`withColumnRenamed()`**
# MAGIC Returns a new DataFrame with a column renamed.

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val locationDf = eventsDf.withColumnRenamed("geo","location")
# MAGIC display(locationDf)

# COMMAND ----------

# MAGIC %md ### Subset Rows
# MAGIC Use DataFrame transformations to subset rows

# COMMAND ----------

# MAGIC %md #### **`filter()`**
# MAGIC Filters rows using the given SQL expression or column based condition.
# MAGIC 
# MAGIC ##### Alias: **`where`**

# COMMAND ----------

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val purchasesDf = eventsDf.filter("ecommerce.total_item_quantity > 0")
# MAGIC display(purchasesDf)

# COMMAND ----------

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val revenueDf = eventsDf.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull)
# MAGIC 
# MAGIC display(revenueDf)

# COMMAND ----------

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val colFilter = (col("traffic_source") =!= lit("direct")) && (col("device") === lit("Android"))
# MAGIC val androidDf = eventsDf.where(colFilter)
# MAGIC display(androidDf)

# COMMAND ----------

# MAGIC %scala
# MAGIC col("traffic_source") =!= "direct"

# COMMAND ----------

# MAGIC %md #### **`dropDuplicates()`**
# MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
# MAGIC 
# MAGIC ##### Alias: **`distinct`**

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

events_df.distinct().explain(mode="formatted")

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])
display(distinct_users_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val distinctUsersDf = eventsDf.dropDuplicates("user_id")
# MAGIC display(distinctUsersDf)

# COMMAND ----------

# MAGIC %md #### **`limit()`**
# MAGIC Returns a new DataFrame by taking the first n rows.

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# MAGIC %md ### Sort rows
# MAGIC Use DataFrame transformations to sort rows

# COMMAND ----------

# MAGIC %md #### **`sort()`**
# MAGIC Returns a new DataFrame sorted by the given columns or expressions.
# MAGIC 
# MAGIC ##### Alias: **`orderBy`**

# COMMAND ----------

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC col("user_first_touch_timestamp").desc_nulls_first

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
