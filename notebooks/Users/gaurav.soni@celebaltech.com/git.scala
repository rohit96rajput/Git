// Databricks notebook source


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.Row

// COMMAND ----------

df.write.mode.saveAsTable("ds")
val
val
val
