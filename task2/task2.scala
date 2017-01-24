//task2.scala

import org.apache.spark.sql.types._
var hrSchema = StructType(Array(StructField("IBT", DoubleType, true),StructField("stimulus_number", LongType , true) ))
// $USER = root

// load the 3 csv files into sep dataframes
val df1 = sqlContext.read.format("com.databricks.spark.csv").schema(hrSchema).option("header","true").option("delimiter",",").option("nullValue","").option("treatEmptyValuesAsNulls","true").load("/user/root/data/1/hr/hr.csv")
val df2 = sqlContext.read.format("com.databricks.spark.csv").schema(hrSchema).option("header","true").option("delimiter",",").option("nullValue","").option("treatEmptyValuesAsNulls","true").load("/user/root/data/2/hr/hr.csv")
val df3 = sqlContext.read.format("com.databricks.spark.csv").schema(hrSchema).option("header","true").option("delimiter",",").option("nullValue","").option("treatEmptyValuesAsNulls","true").load("/user/root/data/3/hr/hr.csv")

// do the math, groupby stimulus_number -> count mean of the IBT, and drop the lines that are based on frewer then 10 data
val a1 = df1.groupBy("stimulus_number").agg(mean("IBT").alias("IBT_per_stimuli_1"),(count("stimulus_number")>10).alias("c")).filter("c").drop("c")
val a2 = df2.groupBy("stimulus_number").agg(mean("IBT").alias("IBT_per_stimuli_2"),(count("stimulus_number")>10).alias("c")).filter("c").drop("c")
val a3 = df3.groupBy("stimulus_number").agg(mean("IBT").alias("IBT_per_stimuli_3"),(count("stimulus_number")>10).alias("c")).filter("c").drop("c")

// full outher join the results and workaround the column name issue
val a12 = a1.join(a2.withColumnRenamed("stimulus_number","a"),$"a"===$"stimulus_number","outer").drop("a")
val a123= a12.join(a3.withColumnRenamed("stimulus_number","a"),$"a"===$"stimulus_number","outer").drop("a")

///calculate the mean of the 3 values
val coder: ( (Double, Double, Double) =>  Double) = (a1:  Double, a2: Double, a3: Double) => {(a1+a2+a3)/3}
val sqlfunc = udf(coder)
val ret = a123.na.fill(0).withColumn("IBT_per_stimuli_mean", sqlfunc(col("IBT_per_stimuli_1"),col("IBT_per_stimuli_3"),col("IBT_per_stimuli_3"))).sort("stimulus_number")
ret.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/root/data/hr_median.csv")

exit
// test it with
//spark-shell --master yarn-client --driver-memory 2g --executor-memory 2g --executor-cores 4 --packages com.databricks:spark-csv_2.11:1.2.0  -i task2.scala
//hadoop fs -ls "/user/root/data/hr_median.csv"
//hadoop fs -text /user/root/data/hr_median.csv/part-00000
//
