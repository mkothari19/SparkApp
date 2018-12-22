package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import util.Context

object BroadCastDemo extends Context{
case class Employee(name:String, age:Int, depId: String)
case class Department(id: String, name: String)
  def main(args: Array[String]): Unit = {
  
 import spark.implicits._
    val employeesDF = spark.sparkContext.parallelize(Seq( 
    Employee("Mary", 33, "IT"), 
    Employee("Paul", 45, "IT"), 
    Employee("Peter", 26, "MKT"), 
    Employee("Jon", 34, "MKT"), 
    Employee("Sarah", 29, "IT"),
    Employee("Steve", 21, "Intern")
)).toDF()
val departmentsDF = spark.sparkContext.parallelize(Seq( 
    Department("IT", "IT  Department"),
    Department("MKT", "Marketing Department"),
    Department("FIN", "Finance & Controlling")
)).toDF()
employeesDF.createOrReplaceTempView("employee")
val tempDempartment=broadcast(departmentsDF.as("department"))
//Approach 1
employeesDF.join(broadcast(tempDempartment),$"depId"===$"id","inner").show()
 
//Approach 2
tempDempartment.toDF().createTempView("department")

spark.sql("select * from employee e  inner join  department d  on e.depid=d.id").show()

  }
}