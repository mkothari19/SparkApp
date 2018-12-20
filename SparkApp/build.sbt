name:= "uber-case-study"
version:= "1.0"
scalaVersion:= "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
   "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
 "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
   "org.apache.spark" %% "spark-mllib" % "2.2.0" % "runtime"
  

 )
