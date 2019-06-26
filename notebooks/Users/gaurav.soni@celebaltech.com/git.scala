// Databricks notebook source

def sum(a:Int):Int=
{
  var fact=1
  for(i <- 1 to a)
  {
    fact=fact*i
  }
  return fact
}
sum(5)

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
