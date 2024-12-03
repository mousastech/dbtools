-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Using HUDI Format

-- COMMAND ----------

-- DBTITLE 1,Generating Data
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC
-- MAGIC # Sample data
-- MAGIC data = [("1", "Mozuca Data", "mozuca@example.com", "2021-01-01"),
-- MAGIC         ("2", "Mozuca Data", "mozuca@example.com", "2021-02-01")]
-- MAGIC columns = ["userId", "name", "email", "createdAt"]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, schema=columns)
-- MAGIC
-- MAGIC # Specify Hudi options
-- MAGIC hudiOptions = {
-- MAGIC  "hoodie.table.name": "user_profiles",
-- MAGIC  "hoodie.datasource.write.recordkey.field": "userId",
-- MAGIC  "hoodie.datasource.write.precombine.field": "createdAt",
-- MAGIC  "hoodie.datasource.write.operation": "upsert",
-- MAGIC  "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
-- MAGIC  "hoodie.datasource.write.table.name": "user_profiles",
-- MAGIC  "hoodie.datasource.write.partitionpath.field": "",
-- MAGIC  "hoodie.datasource.write.hive_style_partitioning": "true"
-- MAGIC }
-- MAGIC
-- MAGIC # Write data to a Hudi table
-- MAGIC df.write.format("org.apache.hudi"). \
-- MAGIC   options(**hudiOptions). \
-- MAGIC   mode("overwrite"). \
-- MAGIC   save("/mnt/moises.santos@databricks.com/temp")

-- COMMAND ----------

-- DBTITLE 1,Checking data loaded
-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- DBTITLE 1,Option 2 - Another way to do that
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC from pyspark.sql. session import SparkSession
-- MAGIC
-- MAGIC tableName = "viajes"
-- MAGIC columns = [
-- MAGIC   "ts",
-- MAGIC   "uuid",
-- MAGIC   "rider",
-- MAGIC   "driver",
-- MAGIC   "fare",
-- MAGIC   "city"
-- MAGIC ]
-- MAGIC data = [
-- MAGIC   (1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
-- MAGIC   (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
-- MAGIC   (1695046462179, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco"),
-- MAGIC   (1695516137016, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
-- MAGIC   (1695115999911, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
-- MAGIC ]
-- MAGIC inserts = spark.createDataFrame(data).toDF(*columns)
-- MAGIC
-- MAGIC basePath = "/temp/" + tableName
-- MAGIC
-- MAGIC hudi_options = {
-- MAGIC   'hoodie.table.name': tableName
-- MAGIC }
-- MAGIC
-- MAGIC inserts.createOrReplaceTempView("trips_table")

-- COMMAND ----------

-- DBTITLE 1,Checking the  temp view

SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0


-- COMMAND ----------

-- DBTITLE 1,Store table
-- MAGIC %python
-- MAGIC inserts.write \
-- MAGIC     .format("org.apache.hudi") \
-- MAGIC     .options(**hudi_options) \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .save(basePath)

-- COMMAND ----------

-- DBTITLE 1,Reading Hudi file
-- MAGIC %python
-- MAGIC a = spark.read.format("org.apache.hudi").option("hoodie.file.index.enable", "false").load(basePath)
-- MAGIC display(a)
