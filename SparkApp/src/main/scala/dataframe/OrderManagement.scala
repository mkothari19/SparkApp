package dataframe

import org.apache.spark.sql.SparkSession
import java.util.Formatter.DateTime
import org.apache.spark.sql.functions._

object OrderManagement {
  case class orders(orderNumber: Option[Int], orderDate:String,
                    requiredDate: String, shippedDate: String,
                    status: String, comments: String, customerNumber: Option[Int])
  case class orderdetails(orderNumber: Option[Long], productCode: String, quantityOrdered: Option[Int], priceEach: Option[Double], orderLineNumber: Option[Int])

  def main(args: Array[String]): Unit = {
    val format = new java.text.SimpleDateFormat("yyyy-dd-MM")

    val spark = SparkSession.builder().appName("Order Management").master("local[*]").getOrCreate()

    val ordersdata = spark.read.textFile("/Volumes/MYHARDDRIVE/dataset/orders.txt")
    val orderdetaildata = spark.read.textFile("/Volumes/MYHARDDRIVE/dataset/orderdetails.txt")
    import spark.implicits._
    val orderDS = ordersdata.map(x => x.split("\\t")).map(y => orders(
      Some(Integer.parseInt(y(0))),
      y(1), y(2), y(3),
      y(4), y(5), Some(Integer.parseInt(y(6)))))

    val orderDetailDS = orderdetaildata.map(x => x.split("\\t")).map(x => orderdetails(Some(java.lang.Long.parseLong(x(0))), x(1), Some(Integer.parseInt(x(2))), Some(java.lang.Double.parseDouble(x(3))), Some(Integer.parseInt(x(4)))))
   val orderDF= orderDS.toDF()
   val temporders=broadcast(orderDF.as("orders"))
   
  val  orderDetailDF =orderDetailDS.toDF()
  temporders.createOrReplaceTempView("orders")
  orderDetailDF.createOrReplaceTempView("orderdetails")
 // val case1=spark.sql("select * from orders") 
   val case1=spark.sql("SELECT status,count(status) as count,sum(priceEach*quantityOrdered) as totalsale from orders INNER JOIN orderdetails using(orderNumber) group by status")
   case1.show
   val case2=spark.sql("""select date_format(shippedDate,"yyy") as year,sum(priceEach*quantityOrdered) as totalsale  from orders o INNER JOIN orderdetails od using(orderNumber) where o.status<>'Cancelled' group by year """)
   case2.show
  
  }
}