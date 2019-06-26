// Databricks notebook source
// DBTITLE 1,Generate data1 for join 
import org.apache.spark.sql.types._

val array = for (i <- List.range(1, 10000000)) yield Row(i, i % 2)
  
val schema =
  StructType(
    StructField("num1", IntegerType, true) ::
    StructField("bit1", IntegerType, true) :: Nil)
val dataFrame1 = sqlContext.createDataFrame(sc.parallelize(array,2), schema)
//dataFrame1.write.mode(SaveMode.Overwrite).saveAsTable("large_table_1")

// COMMAND ----------

// DBTITLE 1,Generate data2 for join 
import org.apache.spark.sql.types._

val array2 = for (i <- List.range(1, 10000000)) yield Row(i, i % 3)
  
val schema2 =
  StructType(
    StructField("num2", IntegerType, true) ::
    StructField("bit2", IntegerType, true) :: Nil)

val dataFrame2 = sqlContext.createDataFrame(sc.parallelize(array2, 200), schema2)
dataFrame2.write.mode(SaveMode.Overwrite).saveAsTable("large_table_3")

// COMMAND ----------

// DBTITLE 1,Join both data1 and data2
// MAGIC %sql 
// MAGIC create table large_joined_table as select * from large_table_1 join large_table_3 on large_table_1.num1 = large_table_3.num2

// COMMAND ----------

// DBTITLE 1,To show number of partitions
println(dataFrame1.queryExecution.toRdd.getNumPartitions)

// COMMAND ----------

// DBTITLE 1,write data1 into a table for bucketing
spark.read.table("large_table_1").write.mode(SaveMode.Overwrite).bucketBy(10, "num1").saveAsTable("bucketed_large_table")

// COMMAND ----------

// DBTITLE 1,write data2 into a table for bucketing
spark.read.table("large_table_3").write.mode(SaveMode.Overwrite).bucketBy(10, "num2").saveAsTable("bucketed_large_table2")

// COMMAND ----------

// DBTITLE 1,Join data1 and data2 with bucketing
// MAGIC %sql 
// MAGIC create table bucketed_large_joined_tables as select * from bucketed_large_table join bucketed_large_table2 
// MAGIC on bucketed_large_table.num1 = bucketed_large_table2.num2

// COMMAND ----------

// DBTITLE 1,Generate data for implement window function
case class Salary(depName: String, empNo: Long, salary: Long)
val empsalary = Seq(
  Salary("sales", 1, 5000),
  Salary("personnel", 2, 3900),
  Salary("sales", 3, 4800),
  Salary("sales", 4, 4800),
  Salary("personnel", 5, 3500),
  Salary("develop", 7, 4200),
  Salary("develop", 8, 6000),
  Salary("develop", 9, 4500),
  Salary("develop", 10, 5200),
  Salary("develop", 11, 5200)).toDS


// COMMAND ----------

display(empsalary)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val C = Window.partitionBy('depName)

// COMMAND ----------

display(empsalary.withColumn("avg", avg("salary") over byDepName))

// COMMAND ----------

// DBTITLE 1,RANK
val byDepnameSalaryDesc = Window.partitionBy('depName).orderBy('salary desc)

// COMMAND ----------

val rankByDepname = rank().over(byDepnameSalaryDesc)

// COMMAND ----------

display(empsalary.select('*, rankByDepname as 'rank))

// COMMAND ----------

// DBTITLE 1,WINDOW PARTITION BY
val byHTokens = Window.partitionBy('depName startsWith "s")

// COMMAND ----------

val result = empsalary.select('*, sum("salary") over byHTokens as "sum over s empsalary").orderBy("salary")


// COMMAND ----------

display(result)

// COMMAND ----------

// DBTITLE 1,lag window function
val buckets = spark.range(9).withColumn("bucket", 'id % 3)

// COMMAND ----------

val dataset = buckets.union(buckets)

// COMMAND ----------

display(dataset)

// COMMAND ----------

val windowSpec = Window.partitionBy('bucket).orderBy('id)

// COMMAND ----------

display(buckets.withColumn("lag", lag('id, 1) over windowSpec))

// COMMAND ----------

// DBTITLE 1,lead window function
display(buckets.withColumn("lead", lead('id, 1) over windowSpec))