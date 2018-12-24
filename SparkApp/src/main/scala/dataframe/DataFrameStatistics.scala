package dataframe

import util.Context
import org.apache.spark.sql.functions._

object DataFrameStatistics extends App with Context {
  case class orders(orderNumber: Option[Int], orderDate:String,
                    requiredDate: String, shippedDate: String,
                    status: String, comments: String, customerNumber: Option[Int])
  case class orderdetails(orderNumber: Option[Long], productCode: String, quantityOrdered: Option[Int], priceEach: Option[Double], orderLineNumber: Option[Int])
 val ds1 = spark.read.textFile("/Volumes/MYHARDDRIVE/dataset/orders.txt")
    val ds2 = spark.read.textFile("/Volumes/MYHARDDRIVE/dataset/orderdetails.txt")
     import spark.implicits._
    val orderDS = ds1.map(x => x.split("\\t")).map(y => orders(
      Some(Integer.parseInt(y(0))),
      y(1), y(2), y(3),
      y(4), y(5), Some(Integer.parseInt(y(6)))))
   val orderDF=orderDS.toDF()
    val orderDetailDS = ds2.map(x => x.split("\\t")).map(x => orderdetails(Some(java.lang.Long.parseLong(x(0))), x(1), Some(Integer.parseInt(x(2))), Some(java.lang.Double.parseDouble(x(3))), Some(Integer.parseInt(x(4)))))
    val orderDetailsDF=orderDetailDS.toDF()
   
    //Average
    orderDetailsDF.select(avg("priceEach")).show
    //Maximun
    orderDetailsDF.select(max("priceEach")).show
    //Minimum
    orderDetailsDF.select(min("priceEach")).show
    //Mean
     orderDetailsDF.select(mean("priceEach")).show
     //Sum
        orderDetailsDF.select(sum("priceEach")).show
     
}