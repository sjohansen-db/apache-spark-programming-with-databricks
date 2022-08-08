# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Abandoned Carts Lab
# MAGIC Get abandoned cart items for email without purchases.
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`sales_df`**, **`users_df`**, and **`events_df`**.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.read.format("delta").load(sales_path)
display(sales_df)

# COMMAND ----------

print(sales_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val salesPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/sales/sales.delta"
# MAGIC 
# MAGIC val salesDf = spark.read.format("delta").load(salesPath)
# MAGIC 
# MAGIC display(salesDf)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.read.format("delta").load(users_path)
display(users_df)

# COMMAND ----------

print(users_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val usersPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/users/users.delta"
# MAGIC 
# MAGIC val usersDf = spark.read.format("delta").load(usersPath)
# MAGIC 
# MAGIC display(usersDf)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.read.format("delta").load(events_path)
display(events_df)

# COMMAND ----------

print(events_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val eventsPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/events/events.delta"
# MAGIC 
# MAGIC val eventsDf = spark.read.format("delta").load(eventsPath)
# MAGIC 
# MAGIC display(eventsDf)

# COMMAND ----------

# MAGIC %md ### 1: Get emails of converted users from transactions
# MAGIC - Select the **`email`** column in **`sales_df`** and remove duplicates
# MAGIC - Add a new column **`converted`** with the value **`True`** for all rows
# MAGIC 
# MAGIC Save the result as **`converted_users_df`**.

# COMMAND ----------

from pyspark.sql.functions import *

converted_users_df = (sales_df
                      .select(col("email"))
                      .dropDuplicates().withColumn("converted", lit(True))
                     )

display(converted_users_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val convertedUsersDf = salesDf
# MAGIC   .select(col("email"))
# MAGIC   .dropDuplicates
# MAGIC   .withColumn("converted", lit(true))
# MAGIC 
# MAGIC display(convertedUsersDf)

# COMMAND ----------

# MAGIC %md #### 1.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 210370

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("email", "converted")
# MAGIC 
# MAGIC val expectedCount = 210370
# MAGIC 
# MAGIC assert(convertedUsersDf.columns.toList == expectedColumns)
# MAGIC assert(convertedUsersDf.count == expectedCount)
# MAGIC assert(convertedUsersDf.select(col("converted")).first.getBoolean(0) == true)
# MAGIC 
# MAGIC print("All tests pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Join emails with user IDs
# MAGIC - Perform an outer join on **`converted_users_df`** and **`users_df`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC 
# MAGIC Save the result as **`conversions_df`**.

# COMMAND ----------

conversions_df = (users_df
                  .join(converted_users_df, ['email'], "fullouter")
                  .filter(col('email').isNotNull())
                  .na.fill(False, ['converted'])
                 )
display(conversions_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val conversionsDf = usersDf
# MAGIC   .join(convertedUsersDf, List("email"), "fullouter")
# MAGIC   .filter(col("email").isNotNull)
# MAGIC   .na.fill(false, List("converted"))
# MAGIC 
# MAGIC display(conversionsDf)

# COMMAND ----------

# MAGIC %md #### 2.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "user_id", "user_first_touch_timestamp", "converted"]

expected_count = 782749

expected_false_count = 572379

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("email", "user_id", "user_first_touch_timestamp", "converted")
# MAGIC val expectedCount = 782749L
# MAGIC val expectedFalseCount = 572379L
# MAGIC 
# MAGIC assert(conversionsDf.columns.toList == expectedColumns)
# MAGIC assert(conversionsDf.filter(col("email").isNull).count == 0L)
# MAGIC assert(conversionsDf.count == expectedCount)
# MAGIC assert(conversionsDf.filter(col("converted") === false).count == expectedFalseCount)
# MAGIC 
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Get cart item history for each user
# MAGIC - Explode the **`items`** field in **`events_df`** with the results replacing the existing **`items`** field
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect a set of all **`items.item_id`** objects for each user and alias the column to "cart"
# MAGIC 
# MAGIC Save the result as **`carts_df`**.

# COMMAND ----------

carts_df = (events_df
            .withColumn("items", explode(col("items")))
            .groupBy("user_id")
            .agg(collect_set("items.item_id").alias("cart"))
)
display(carts_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cartsDf = eventsDf
# MAGIC   .withColumn("items", explode(col("items")))
# MAGIC   .groupBy("user_id")
# MAGIC   .agg(collect_set("items.item_id").alias("cart"))
# MAGIC 
# MAGIC display(cartsDf)

# COMMAND ----------

# MAGIC %md #### 3.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 488403

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("user_id", "cart")
# MAGIC val expectedCount = 488403L
# MAGIC 
# MAGIC assert(cartsDf.columns.toList == expectedColumns)
# MAGIC assert(cartsDf.count == expectedCount)
# MAGIC assert(cartsDf.select(col("user_id")).dropDuplicates.count == expectedCount)
# MAGIC 
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4: Join cart item history with emails
# MAGIC - Perform a left join on **`conversions_df`** and **`carts_df`** on the **`user_id`** field
# MAGIC 
# MAGIC Save result as **`email_carts_df`**.

# COMMAND ----------

email_carts_df = conversions_df.join(carts_df, ['user_id'], 'left')
display(email_carts_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val emailCartsDf = conversionsDf.join(cartsDf, List("user_id"), "left")
# MAGIC 
# MAGIC display(emailCartsDf)

# COMMAND ----------

# MAGIC %md #### 4.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expected_count = 782749

expected_cart_null_count = 397799

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("user_id", "email", "user_first_touch_timestamp", "converted", "cart")
# MAGIC val expectedCount = 782749L
# MAGIC val expectedCartNullCount = 397799L
# MAGIC 
# MAGIC assert(emailCartsDf.columns.toList == expectedColumns)
# MAGIC assert(emailCartsDf.count == expectedCount)
# MAGIC assert(emailCartsDf.filter(col("cart").isNull).count == expectedCartNullCount)
# MAGIC 
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5: Filter for emails with abandoned cart items
# MAGIC - Filter **`email_carts_df`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC 
# MAGIC Save result as **`abandoned_carts_df`**.

# COMMAND ----------

abandoned_carts_df = (email_carts_df
                      .filter((col("converted") == False) & col("cart").isNotNull())
)
display(abandoned_carts_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val abandonedCartsDf = emailCartsDf.filter(col("converted") === false && col("cart").isNotNull)
# MAGIC 
# MAGIC display(abandonedCartsDf)

# COMMAND ----------

# MAGIC %md #### 5.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expected_count = 204272

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("user_id", "email", "user_first_touch_timestamp", "converted", "cart")
# MAGIC val expectedCount = 204272L
# MAGIC 
# MAGIC assert(abandonedCartsDf.columns.toList == expectedColumns)
# MAGIC assert(abandonedCartsDf.count == expectedCount)
# MAGIC 
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6: Bonus Activity
# MAGIC Plot number of abandoned cart items by product

# COMMAND ----------

abandoned_items_df = (abandoned_carts_df
                      .select(explode(col("cart")).alias("items"))
                      .groupBy("items")
                      .count()
                     )
display(abandoned_items_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val abandonedItemsDf = abandonedCartsDf
# MAGIC   .select(explode(col("cart")).alias("items"))
# MAGIC   .groupBy("items")
# MAGIC   .count
# MAGIC 
# MAGIC display(abandonedItemsDf)

# COMMAND ----------

# MAGIC %md #### 6.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

abandoned_items_df.count()

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val expectedColumns = List("items", "count")
# MAGIC val expectedCount = 12L
# MAGIC 
# MAGIC assert(abandonedItemsDf.count == expectedCount)
# MAGIC assert(abandonedItemsDf.columns.toList == expectedColumns)
# MAGIC 
# MAGIC println("All tests pass")

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
