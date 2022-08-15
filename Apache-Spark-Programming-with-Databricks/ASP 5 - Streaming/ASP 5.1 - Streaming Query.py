# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Query
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Build streaming DataFrames
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC 1. Monitor streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### Build streaming DataFrames
# MAGIC 
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(events_path)
     )

df.isStreaming

# COMMAND ----------

print(events_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val eventsPath = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/events/events.delta"
# MAGIC 
# MAGIC val df = spark.readStream.option("maxFilesPerTrigger", "1").format("delta").load(eventsPath)
# MAGIC 
# MAGIC df.isStreaming

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/datasets/events/events.delta

# COMMAND ----------

# MAGIC %md
# MAGIC Apply some transformations, producing new streaming DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

email_traffic_df = (df
                    .filter(col("traffic_source") == "email")
                    .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                    .select("user_id", "event_timestamp", "mobile")
                   )

email_traffic_df.isStreaming

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val emailTrafficDf = df
# MAGIC   .filter(col("traffic_source") === "email")
# MAGIC   .withColumn("mobile", col("device").isin(lit("iOS"), lit("Andriod")))
# MAGIC   .select("user_id", "event_timestamp", "mobile")
# MAGIC 
# MAGIC emailTrafficDf.isStreaming

# COMMAND ----------

# MAGIC %md ### Write streaming query results
# MAGIC 
# MAGIC Take the final streaming DataFrame (our result table) and write it to a file sink in "append" mode.

# COMMAND ----------

checkpoint_path = f"{working_dir}/email_traffic/checkpoint"
output_path = f"{working_dir}/email_traffic/output"

devices_query = (email_traffic_df
                 .writeStream
                 .outputMode("append")
                 .format("delta")
                 .queryName("email_traffic")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation", checkpoint_path)
                 .start(output_path)
                )

# COMMAND ----------

print(working_dir)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC 
# MAGIC val workingDir = "dbfs:/user/steve.johansen@databricks.com/dbacademy/aspwd/asp_5_1_streaming_query"
# MAGIC val checkPointPath = s"${workingDir}/email_traffic/checkpoint_s"
# MAGIC val outputPath = s"${workingDir}/email_traffic/outpus_s"
# MAGIC 
# MAGIC val devicesQuery = emailTrafficDf
# MAGIC   .writeStream
# MAGIC   .outputMode("append")
# MAGIC   .format("delta")
# MAGIC   .queryName("email_traffic_s")
# MAGIC   .trigger(Trigger.ProcessingTime("1 second"))
# MAGIC   .option("checkpointLocation", checkPointPath)
# MAGIC   .start(outputPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor streaming query
# MAGIC 
# MAGIC Use the streaming query "handle" to monitor and control it.

# COMMAND ----------

devices_query.id

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC devicesQuery.stop

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC 
# MAGIC devicesQuery.awaitTermination

# COMMAND ----------

# MAGIC %md ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
