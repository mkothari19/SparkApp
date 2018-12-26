package structurestreaming

import util.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.OutputMode
import scala.reflect.api.materializeTypeTag

object SystemHealth extends App with Context{
  case class syshealth(event:String,mem:String,disk:String,cpu:String)
  val inputstream=spark.readStream.format("kafka")
   .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "systemhealth")
  .load()
  import spark.implicits._
  val inputVal=inputstream.selectExpr("CAST(value as String)").as[(String)]

val syshealthDS=  inputVal.map(x=>x.split("\t\t")).map(r=>syshealth(r(0),r(1),r(2),r(3)))
val syshealthDF=syshealthDS.toDF()
val finalDF=syshealthDF.select(unix_timestamp($"event","dd-MM-yyy HH:mm:ss").cast(TimestampType).as("timestamp"), syshealthDF.col("mem").cast("double"),syshealthDF.col("disk"),syshealthDF.col("cpu").cast("double"))
val windowDF= finalDF.withWatermark("timestamp","10 minutes").
              groupBy(window($"timestamp","10 seconds","5 seconds")).
              agg(avg($"mem").as("avgmem") ,max($"disk").as("maxdisk"),avg($"cpu").as("avgcpu"))
val query=windowDF.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").
          option("topic","systemhealthout").option("checkpointLocation", "/Volumes/MYHARDDRIVE/checkpoint/systemhealth").outputMode(OutputMode.Complete()) .start()
query.awaitTermination()



}