package dataframe

import util.Context

object DataFrameOperation extends Context{
   case class orders(orderNumber: Option[Int], orderDate:String,
                    requiredDate: String, shippedDate: String,
                    status: String, comments: String, customerNumber: Option[Int])
  case class orderdetails(orderNumber: Option[Long], productCode: String, quantityOrdered: Option[Int], priceEach: Option[Double], orderLineNumber: Option[Int])

  def main(args: Array[String]): Unit = {
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
    orderDF.show()
    orderDetailsDF.show
   // filter by column value of a dataframe
    orderDF.filter($"status"==="Cancelled").show(10)
    //Number of cancel order
    println(s"Number of cancel order=${orderDF.filter("status=='Cancelled'").count()}")
  // DataFrame SQL like query
    orderDF.filter("status like 'S%'").show(10)
    //DateFrame filter chaining
    orderDF.filter("status like 'S%'").filter("shippeddate=='2003-07-30'").show
   //DateFrame SQL in query
    orderDF.filter("customernumber in(124,381)").show(10)
    //Group by with filter and orderBy
    orderDF.groupBy("status").count().filter("count>1").orderBy("status").show(10)
    orderDF.printSchema()
    //Cast DataFrame column into spacific data type
   val orderDFNew= orderDF.select(orderDF.col("ordernumber").cast("integer"),orderDF.col("orderdate").cast("date"),orderDF.col("requireddate").cast("date"),
                  orderDF.col("shippeddate").cast("date"),orderDF.col("status"),orderDF.col("comments"),orderDF.col("customerNumber").cast("integer"))
       orderDFNew.printSchema()
       
   orderDetailDS.show
   orderDFNew.show()
   // Join 
orderDFNew.join(orderDetailsDF,"ordernumber").show()

//Join on explicit column
orderDFNew.join(orderDetailsDF,orderDFNew("ordernumber")===orderDetailsDF("ordernumber")).show
 //Inner Join
orderDFNew.join(orderDetailsDF,orderDFNew("ordernumber")===orderDetailsDF("ordernumber"),"inner").show
//left outer
orderDFNew.join(orderDetailsDF,orderDFNew("ordernumber")===orderDetailsDF("ordernumber"),"left_outer").show
//right outer
orderDFNew.join(orderDetailsDF,orderDFNew("ordernumber")===orderDetailsDF("ordernumber"),"right_outer").show
//User defined function:UDF
orderDetailsDF.show()
orderDetailsDF.createOrReplaceTempView("orderDetails")
   spark.udf.register("rate_change",rateChange _)
   spark.sql("select ordernumber,productcode,quantityOrdered,priceEach as oldprice ,rate_change(priceEach) as newprice from orderDetails").show()
 
   
   }
 
   def rateChange(price:Double):Double=
   {
     val per=10
     val changePrice=price+(price/per)
     return changePrice
   }
  
}