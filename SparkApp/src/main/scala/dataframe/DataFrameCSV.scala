package dataframe

import org.apache.spark.sql.SparkSession
 
object DataFrameCSV {
  def main(args: Array[String]): Unit = {
   if(args.length!=1){
     println("Please provide input file path")
   //System.exit(1)
   }
   val path="/Volumes/MYHARDDRIVE/sparkdemoapp/SparkApp/dataset/autos.csv"
   val spark=SparkSession.builder().appName("DATA CSV PROCESSING").master("local[4]").getOrCreate()
   val data=spark.read.format("com.databricks.spark.csv").option("header","true").load(path) 
    data.show
    data.createOrReplaceTempView("Vehicle")
    // Registration expire when fuel type is diesel and vehicle old then 1988 
    
    val fueltypedf=spark.sql("""select name,price,kilometer,brand,yearOfRegistration,fueltype from vehicle where fueltype!='null' and fueltype='diesel'  and (date_format(current_date(),"yyyy")-year(yearOfRegistration))>=10""")
    
   fueltypedf.show
  
  }
}