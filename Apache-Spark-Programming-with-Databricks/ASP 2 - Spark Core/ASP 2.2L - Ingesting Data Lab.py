# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingesting Data Lab
# MAGIC 
# MAGIC Read in CSV files containing products data.
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. Read with infer schema
# MAGIC 2. Read with user-defined schema
# MAGIC 3. Read with schema as DDL formatted string
# MAGIC 4. Write using Delta format

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method **`fs.head`** with the filepath provided in the variable **`single_product_cs_fil_path`**
# MAGIC - Create **`products_df`** by reading from CSV files located in the filepath provided in the variable **`products_csv_path`**
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

single_product_csv_file_path = f"{datasets_dir}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = f"{datasets_dir}/products/products.csv"
products_df = spark.read.csv(products_csv_path, header=True, inferSchema=True)

products_df.printSchema()

# COMMAND ----------

print(datasets_dir)

# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------

# MAGIC %scala
# MAGIC val datasetsDir = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets"
# MAGIC 
# MAGIC val singleFileCsvPath = s"${datasetsDir}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
# MAGIC 
# MAGIC println(dbutils.fs.head(singleFileCsvPath))
# MAGIC 
# MAGIC val productsCsvPath = s"${datasetsDir}/products/products.csv"
# MAGIC 
# MAGIC val productsDf = spark.read.option("header", "true").option("inferSchema", "true").csv(productsCsvPath)
# MAGIC 
# MAGIC productsDf.printSchema

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC assert(productsDf.count == 12)
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md ### 2. Read with user-defined schema
# MAGIC Define schema by creating a **`StructType`** with column names and data types

# COMMAND ----------

from pyspark.sql.types import *

user_defined_schema = StructType([
  StructField("item_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("price", DoubleType(), True)
])

products_df2 = spark.read.schema(user_defined_schema).csv(products_csv_path, header=True)
display(products_df2)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val userDefinedSchema = StructType(List(
# MAGIC   StructField("item_id", StringType, nullable=true),
# MAGIC   StructField("name", StringType, nullable=true),
# MAGIC   StructField("price", DoubleType, nullable=true)
# MAGIC ))
# MAGIC 
# MAGIC val productsDf2 = spark.read.schema(userDefinedSchema).option("header", "true").csv(productsCsvPath)
# MAGIC display(productsDf2)

# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC assert(userDefinedSchema.fields.toList.map(_.name) == List("item_id", "name", "price"))
# MAGIC println("All tests pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.Row
# MAGIC 
# MAGIC val expected1 = Row("M_STAN_Q", "Standard Queen Mattress", 1045.0)
# MAGIC val result1 = productsDf2.head(1).headOption
# MAGIC 
# MAGIC assert(Option(expected1) == result1)
# MAGIC print("All tests pass")

# COMMAND ----------

# MAGIC %md ### 3. Read with DDL formatted string

# COMMAND ----------

ddl_schema = """
item_id STRING,
name STRING,
price DOUBLE
"""

products_df3 = spark.read.schema(ddl_schema).csv(products_csv_path, header=True)
display(products_df3)

# COMMAND ----------

# MAGIC %scala
# MAGIC val ddlSchema = """
# MAGIC item_id STRING,
# MAGIC name STRING,
# MAGIC price DOUBLE
# MAGIC """
# MAGIC 
# MAGIC val productsDf3 = spark.read.schema(ddlSchema).option("header", "true").csv(productsCsvPath)
# MAGIC display(productsDf3)

# COMMAND ----------

# MAGIC %md **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC assert(productsDf3.count == 12)
# MAGIC println("All tests pass")

# COMMAND ----------

# MAGIC %md ### 4. Write to Delta
# MAGIC Write **`products_df`** to the filepath provided in the variable **`products_output_path`**

# COMMAND ----------

products_output_path = working_dir + "/delta/products"
products_df.write.format("delta").mode("overwrite").save(products_output_path)

# COMMAND ----------

print(working_dir)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val workingDir = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/asp_2_2l_ingesting_data_lab"
# MAGIC 
# MAGIC val productsOutputPath = s"${workingDir}/delta/products"
# MAGIC productsDf.write.format("delta").mode(SaveMode.Overwrite).save(productsOutputPath)

# COMMAND ----------

# MAGIC %md **4.1: CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# MAGIC %scala
# MAGIC val verifyFiles = dbutils.fs.ls(productsOutputPath)
# MAGIC 
# MAGIC assert(verifyFiles.toList.map(_.name).exists(_ == "_delta_log/"))
# MAGIC assert(verifyFiles.toList.map(_.name).filter(_.endsWith(".parquet")).size > 0)
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
