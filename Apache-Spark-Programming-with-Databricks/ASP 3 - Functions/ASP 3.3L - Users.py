# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# Read in the dataset for the lab, along with all functions

from pyspark.sql.functions import *

df = spark.read.format("delta").load(sales_path)
display(df)

# COMMAND ----------

print(sales_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val salesPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/sales/sales.delta"
# MAGIC val df = spark.read.format("delta").load(salesPath)
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md ### 1. Extract item details from purchases
# MAGIC 
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Select the **`email`** and **`item.item_name`** fields
# MAGIC - Split the words in **`item_name`** into an array and alias the column to "details"
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`details_df`**.

# COMMAND ----------

from pyspark.sql.functions import *

details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
display(details_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val detailsDf = df
# MAGIC   .withColumn("items", explode(col("items")))
# MAGIC   .select("email", "items.item_name")
# MAGIC   .withColumn("details", split(col("item_name"), " "))
# MAGIC 
# MAGIC display(detailsDf)

# COMMAND ----------

# MAGIC %md So you can see that our **`details`** column is now an array containing the quality, size, and object type. 

# COMMAND ----------

# MAGIC %md ### 2. Extract size and quality options from mattress purchases
# MAGIC 
# MAGIC - Filter **`details_df`** for records where **`details`** contains "Mattress"
# MAGIC - Add a **`size`** column by extracting the element at position 2
# MAGIC - Add a **`quality`** column by extracting the element at position 1
# MAGIC 
# MAGIC Save the result as **`mattress_df`**.

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2))
               .withColumn("quality", element_at(col("details"), 1))
              )
display(mattress_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val mattressDf = detailsDf
# MAGIC   .where(array_contains(col("details"), "Mattress"))
# MAGIC   .withColumn("size", element_at(col("details"), 2))
# MAGIC   .withColumn("quality", element_at(col("details"), 1))
# MAGIC 
# MAGIC display(mattressDf)

# COMMAND ----------

# MAGIC %md Next we're going to do the same thing for pillow purchases.

# COMMAND ----------

# MAGIC %md ### 3. Extract size and quality options from pillow purchases
# MAGIC - Filter **`details_df`** for records where **`details`** contains "Pillow"
# MAGIC - Add a **`size`** column by extracting the element at position 1
# MAGIC - Add a **`quality`** column by extracting the element at position 2
# MAGIC 
# MAGIC Note the positions of **`size`** and **`quality`** are switched for mattresses and pillows.
# MAGIC 
# MAGIC Save result as **`pillow_df`**.

# COMMAND ----------

pillow_df = (details_df
             .filter(array_contains(col("details"), "Pillow"))
             .withColumn("size", element_at(col("details"), 1))
             .withColumn("quality", element_at(col("details"), 2))
            )
display(pillow_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val pillowDf = detailsDf
# MAGIC   .where(array_contains(col("details"), "Pillow"))
# MAGIC   .withColumn("size", element_at(col("details"), 1))
# MAGIC   .withColumn("quality", element_at(col("details"), 2))
# MAGIC 
# MAGIC display(pillowDf)

# COMMAND ----------

# MAGIC %md ### 4. Combine data for mattress and pillows
# MAGIC 
# MAGIC - Perform a union on **`mattress_df`** and **`pillow_df`** by column names
# MAGIC - Drop the **`details`** column
# MAGIC 
# MAGIC Save the result as **`union_df`**.

# COMMAND ----------

union_df = mattress_df.unionByName(pillow_df).drop("details")
display(union_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val unionDf = mattressDf.unionByName(pillowDf).drop(col("details"))
# MAGIC 
# MAGIC display(unionDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. List all size and quality options bought by each user
# MAGIC 
# MAGIC - Group rows in **`union_df`** by **`email`**
# MAGIC   - Collect the set of all items in **`size`** for each user and alias the column to "size options"
# MAGIC   - Collect the set of all items in **`quality`** for each user and alias the column to "quality options"
# MAGIC 
# MAGIC Save the result as **`options_df`**.

# COMMAND ----------

options_df = (union_df
              .groupBy("email")
              .agg(collect_set("size").alias("size options"),
                   collect_set("quality").alias("quality options"))
             )
display(options_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val optionsDf = unionDf
# MAGIC   .groupBy("email")
# MAGIC   .agg(
# MAGIC     collect_set(col("size")).alias("size options"),
# MAGIC     collect_set(col("quality").alias("quality options"))
# MAGIC   )
# MAGIC   
# MAGIC display(optionsDf)

# COMMAND ----------

# MAGIC %md ### Clean up classroom
# MAGIC 
# MAGIC And lastly, we'll clean up the classroom.

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
