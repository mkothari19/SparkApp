# SparkApp
1.Baisc dataframe program
  Application featch records from CSV file and perform some basic aggregation logic and save into parquet file format."\n"
   Have a look DataFrameCSV under src folder 

2.How to  use broad cast variable
 Have a look into class BroadCastDemo.scala 
 
3.OrderManagement
 Where to use broad cast varibale
 
4. Veeva Intigration
    As veeva is salesforce  crm tool for health care domain.Here i need to intigrate sales force veeva instance to client system.
    Use Case : SOAP intigration with HDFS and limitation was client not ready to provide WSDL information they only share soap request format and api gateway credential to communicate with service layer.This example show how to call webservice  from spark.
    
5.Structure Streaming 
   Here i created simple structure streaming pipeline using kafka,spark,elastic search and kibana.
   Step :
   
    1. Start kafka 
       a) ./bin/zookeeper-server-start.sh config/zookeeper.properties
       
       b) ./bin/kafka-server-start.sh config/server.properties
       
       c) create kafka topic using below command 
       
       ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic            systemhealth
       
       d) I created shell script name systemhealth.sh and this availabel in dataset folder.This script work as a producer to             generate message like system memory,cpu and disk usages.Below is the way to execute the script
        /Volumes/MYHARDDRIVE/dataset/systemhealth.sh|./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic systemhealth
    
    
     2. Run spark job available in package dataframe.structurestream.SystemHealth
    
    This job perform some aggregation top of the available data set and send messages to kafka topic name(create new topic like systemhealthoutput)
     3.  Start  services of elasticsearch,logstash and kibana
    
    a) Create conf file to generate indexs for available mesaages in kafka topic.File system.conf available in dataset folder
    
    b)Run logstash script to create a index
       logstash -f /Volumes/MYHARDDRIVE/dataset/logstashconfig/systemhealth/system.conf
    
    4. Open kibana dash board a see your messages are indexed.Now you can visulize your data.  
        
