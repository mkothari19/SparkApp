package dataframe

import java.io.{ DataOutputStream, OutputStreamWriter, BufferedWriter }
import java.net.HttpURLConnection
import java.text.SimpleDateFormat
import java.util.Date

import scala.xml.{ Elem, XML }
import scala.collection.mutable.ListBuffer

import java.util.Arrays
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.io.Source.fromURL
import java.util.Properties
import java.util.concurrent.Callable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.InputStream
import com.google.common.base.Stopwatch
import java.util.ArrayList
import util.Context

object VeevaIntigration extends Context{
  
  var responsecount=0
   var prop : Properties = null
   var locale:String=null
    var host: String = null
    var fillocation: String = null
    var headerfile:String=null
  def main(args: Array[String]): Unit = {
   
   
    if (args.length != 4) {
     
      println("Please provide valid below paramter to submit a Job \n (1)Instance locale (2) AVICO SERVICE END POINT \n (3) FILE LOCATION \n)")
//      System.exit(0)
    }
    
  //  locale=args(0).toUpperCase().trim()
    // host = args(1)
    //fillocation = args(2)
   //headerfile=args(3)
   locale="us"
     host ="http://localhost:8888"
    fillocation ="/Volumes/MYHARDDRIVE/sparkdemoapp/SparkApp/dataset/Suggestions_dummy.csv"
    headerfile="/Volumes/MYHARDDRIVE/sparkdemoapp/SparkApp/dataset/header-info.properties"
  
      val path:Path=new Path(headerfile)
      val fs:FileSystem=FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val inputStream:InputStream=fs.open(path);
      prop = new Properties();
      prop.load(inputStream);
     val df = spark.read.format("csv").option(
      "header",
      "true").load(fillocation)
    import spark.implicits._
     val result= df.map(row=>callCreateMasterInvoiceWs(row))  
        val report=  result.rdd.map(x=>x.split("#######")).map(r=>Report(r(0),r(1).toInt,r(2)))
    println(report.first())
       report.toDF().write.csv("/Volumes/MYHARDDRIVE/sparkdemoapp/SparkApp/dataset/veeva")
    
    println("**************TOTAL NUMBER OF REQUEST MADE***************>>"+locale+"==" + df.count())
     println("*************TOTAL NUMBER OF SUCESS RESPONSE  COUNT>>"+locale+ "=="+responsecount)
 
     
  }
  
  def getConnection():HttpURLConnection={
    
     val url = new java.net.URL(host)
  val conn: HttpURLConnection =
      url.openConnection.asInstanceOf[java.net.HttpURLConnection]
      try {
   /*  conn.setRequestMethod("POST")
     conn.setDoOutput(true)
     conn.setRequestProperty("Content-Type",(prop.getProperty("Content-Type-"+locale)).toString())
     conn.setRequestProperty("identifier",prop.getProperty("identifier-"+locale).toString())
     conn.setRequestProperty("authorization",prop.getProperty("authorization-"+locale).toString)
     conn.setRequestProperty("keyid",prop.getProperty("keyid-"+locale).trim().toString())
     conn.setRequestProperty("sfdcversion",prop.getProperty("sfdcversion-"+locale).toString())
     conn.setRequestProperty("cache-control",prop.getProperty("cache-control-"+locale).toString())
  */
      conn.setRequestProperty("Content-Type","text/xml; charset=utf-8")
  
     conn.setRequestProperty("identifier","sfdc-phm-gbl-1")
     conn.setRequestProperty("authorization","Basic VXNlcl9BcGlHd1NmZGNBa3RhbmE6QXBpZ3dTZmRjMTIzIQ==")
     conn.setRequestProperty("keyid","b5a6fee1-5577-4d86-adb8-67ac2f9bc2ca")
     conn.setRequestProperty("sfdcversion","40.0")
     conn.setRequestProperty("cache-control","no-cache")
       conn.connect()
        
        } catch {
      case e: Exception =>
        print("Error to connect  " + e.printStackTrace())
     
    }
      return conn
     
  }
  
  def callCreateMasterInvoiceWs(row: Row): String = {
     
     //   val dateFormatter = new SimpleDateFormat("MM/dd/yyyy")
   // val apiformat = new SimpleDateFormat("yyyy-MM-dd")
    val conn=getConnection
     val body = new StringBuilder
     val storage=new StringBuffer()
      try{
   
    body.append("<ns1:upsert>\n")
    body.append("<ns1:externalIDFieldName>Suggestion_External_Id_vod__c</ns1:externalIDFieldName>\n")
    body.append("<ns1:sObjects>\n")
    body.append("<ns2:type>Suggestion_vod__c</ns2:type>\n")
    body.append("<ns2:Reason_vod__c>" + row.getAs("reason_vod__c") + "</ns2:Reason_vod__c>\n")
    body.append("<ns2:Call_Objective_From_Date_vod__c>" + row.getAs("call_objective_from_date_vod__c") + "</ns2:Call_Objective_From_Date_vod__c>\n")
    body.append("<ns2:Expiration_Date_vod__c>" + row.getAs("expiration_date_vod__c") + "</ns2:Expiration_Date_vod__c>\n")
    body.append("<ns2:Account_vod__c>" + row.getAs("account_vod__c") + "</ns2:Account_vod__c>\n")
    body.append("<ns2:Display_Dismiss_vod__c>" + row.getAs("display_dismiss_vod__c") + "</ns2:Display_Dismiss_vod__c>\n")
    body.append("<ns2:Email_Template_vod__c>" + row.getAs("email_template_vod__c") + "</ns2:Email_Template_vod__c>\n")
    body.append("<ns2:Suggestion_External_Id_vod__c>" + row.getAs("suggestion_external_id_vod__c") + "</ns2:Suggestion_External_Id_vod__c>\n")
    body.append("<ns2:Account_Priority_Score_vod__c>" + row.getAs("account_priority_score_vod__c") + "</ns2:Account_Priority_Score_vod__c>\n")
    body.append("<ns2:Suppress_Reason_vod__c>" + row.getAs("suppress_reason_vod__c") + "</ns2:Suppress_Reason_vod__c>\n")
    body.append("<ns2:Display_Score_vod__c>" + row.getAs("display_score_vod__c") + "</ns2:Display_Score_vod__c>\n")
    //body.append("<ns2:RecordTypeId>" + row.getAs("recordtypeid") + "</ns2:RecordTypeId>\n")
    body.append("<ns2:Priority_vod__c>" + row.getAs("priority_vod__c") + "</ns2:Priority_vod__c>\n")
    body.append("<ns2:No_Homepage_vod__c>" + row.getAs("no_homepage_vod__c") + "</ns2:No_Homepage_vod__c>\n")
    body.append("<ns2:Display_Mark_As_Complete_vod__c>" + row.getAs("display_mark_as_complete_vod__c") + "</ns2:Display_Mark_As_Complete_vod__c>\n")
    body.append("<ns2:Call_Objective_On_By_Default_vod__c>" + row.getAs("call_objective_on_by_default_vod__c") + "</ns2:Call_Objective_On_By_Default_vod__c>\n")
    body.append("<ns2:Call_Objective_To_Date_vod__c>" + row.getAs("call_objective_to_date_vod__c") + "</ns2:Call_Objective_To_Date_vod__c>\n")
    body.append("<ns2:Call_Objective_Record_Type_vod__c>" + row.getAs("call_objective_record_type_vod__c") + "</ns2:Call_Objective_Record_Type_vod__c>\n")
   
    if((!"NULL".equals(row.getAs("call_objective_clm_id_vod__c")))&&(!" ".equals(row.getAs("call_objective_clm_id_vod__c")))&&(!null.equals(row.getAs("call_objective_clm_id_vod__c")))){
        body.append("<ns2:Call_Objective_CLM_ID_vod__c>" + row.getAs("call_objective_clm_id_vod__c") + "</ns2:Call_Objective_CLM_ID_vod__c>\n")
   }
    
    if("TRUE".equals(row.getAs("email_template_vod__c"))){
     body.append("<ns2:Email_Template_ID_vod__c>" + row.getAs("email_template_vod__c") + "</ns2:Email_Template_ID_vod__c>\n")
     body.append("<ns2:Email_Template_ID_vod__c>" + row.getAs("email_template_id_vod__c") + "</ns2:Email_Template_ID_vod__c>\n")
     body.append("<ns2:Email_Template_Vault_ID_vod__c>" + row.getAs("email_template_vault_id_vod__") + "</ns2:Email_Template_Vault_ID_vod__c>\n")
  
    }
    
    if((!"NULL".equals(row.getAs("ownerid")))&& (!" ".equals(row.getAs("ownerid")))&&(row.getAs("ownerid")!=null)){
      body.append("<ns2:OwnerId>" + row.getAs("ownerid") + "</ns2:OwnerId>\n")
   
    }
    
    if((!"NULL".equals(row.getAs("planned_call_date_vod__c")))&& (!" ".equals(row.getAs("planned_call_date_vod__c")))&&(row.getAs("planned_call_date_vod__c")!=null)){
      body.append("<ns2:Planned_Call_Date_vod__c>" +  row.getAs("planned_call_date_vod__c") + "</ns2:Planned_Call_Date_vod__c>\n")
   
    }
     
    if((!"NULL".equals(row.getAs("posted_date_vod__c")))&& (!" ".equals(row.getAs("posted_date_vod__c")))&&(row.getAs("posted_date_vod__c"))!=null){
      body.append("<ns2:Posted_Date_vod__c>" +row.getAs("posted_date_vod__c") + "</ns2:Posted_Date_vod__c>\n")
   
    }
 
      if((!"NULL".equals(row.getAs("title_vod__c")))&& (!" ".equals(row.getAs("title_vod__c")))&&(row.getAs("title_vod__c"))!=null){
      body.append("<ns2:Title_vod__c>" + row.getAs("title_vod__c") + "</ns2:Title_vod__c>\n")
   
    }
      
      if((!"NULL".equals(row.getAs("record_type_name_vod__c")))&& (!" ".equals(row.getAs("record_type_name_vod__c")))&&(row.getAs("record_type_name_vod__c"))!=null){
             body.append("<ns2:Record_Type_Name_vod__c>" + row.getAs("record_type_name_vod__c") + "</ns2:Record_Type_Name_vod__c>\n")
      }
      
    
    body.append("</ns1:sObjects>\n")
    body.append("</ns1:upsert>")

    println("##### request:\n" + wrap(body.toString()))
   val wr = new DataOutputStream(conn.getOutputStream)
   
   val wrappedReq = wrap(body.toString())
    val outs = wrappedReq.getBytes
      wr.writeBytes(wrappedReq)
      wr.flush()
      wr.close()

      val responseCode = conn.getResponseCode
      val resp: Option[Elem]=   Some(XML.load(conn.getInputStream))
        
        if (resp != null && resp.isDefined) {
      println("############RESPONSE CODE############# "+responseCode)    
      println("############ Response#################\n" + resp.get.toString)
    }
    
     storage.append(body.toString())
     storage.append("#######")
     storage.append(responseCode)
      storage.append("#######")
     storage.append(resp.get.toString)
    return storage.toString()
}catch{
       case e: Exception =>
        print("Exception to submit a request: " + e.printStackTrace())
        storage.append(body.toString())
     storage.append("#######")
     storage.append(0)
     storage.append("#######")
     storage.append("WEB SERVICE FAILED TO RESPOND")
     return storage.toString()
    }

  }
  
  case class Report(request:String,reponseCode:Int,response:String)
  

  def wrap(xml: String): String = {
    val buf = new StringBuilder

    buf.append("<ns0:Envelope xmlns:ns0=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns1=\"urn:partner.soap.sforce.com\" xmlns:ns2=\"urn:sobject.partner.soap.sforce.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instanc\">\n")
    buf.append("<ns0:Header>\n")
    buf.append("<ns1:SessionHeader>\n")
    buf.append("<ns1:sessionId>\n")
  //  buf.append("00D5B000000DhKK!AQQAQAFffL6mjk5wzJo4KtG05Qr5_Qy8C0AH8HEu9YZg330ey3Jl7rLjd2SMdFrejsjhDeiEvYBTbP5n1z3WEe3.knGeNlRG")
    buf.append("</ns1:sessionId>\n")
    buf.append("</ns1:SessionHeader>\n")
    buf.append("</ns0:Header>\n")
    buf.append("<ns0:Body>\n")
    buf.append(xml.toString)
    buf.append("</ns0:Body>\n")
    buf.append("</ns0:Envelope>\n")
    buf.toString
    
  }
  

}