### converter.scala

import org.apache.spark.sql.types._
def convert(sqlContext: org.apache.spark.sql.SQLContext,basename: String,, filename: String, schema: StructType, tablename: String){
  val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("header","true").option("delimiter",",").option("nullValue","").option("treatEmptyValuesAsNulls","true").load(basename+filename)
  // now simply write to a parquet file
   df.write.parquet(basename+tablename+"_parquet")
  }

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var hrSchema = StructType(Array(StructField("IBT", DoubleType, true),StructField("stimulus_number", LongType , true) ))
convert(sqlContext,"/user/root/data/1/hr/","hr.csv",hrSchema,"hrSchema")