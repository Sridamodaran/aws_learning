import configparser
def getRdbmsPartData(propfile, sparksess, db, tbl, partcol, lowerbound, upperbound, numpart):
   #getRdbmsPartData('/home/hduser/install/aws/connection_rds.prop', spark, 'inceptezdb', drugs_query, "uniqueid", 1,1000, 4) #250,250,250,450
   #drawback in spark is automated boundary query is not available, we have either create one or we have hardcode
   #spark provides you the feature of unification (in one prog do ingestion, curation, load) and federation (connect to any src/tgt in a single application)
   config = configparser.ConfigParser()
   config.read(propfile)
   driver = config.get("DBCRED", 'driver')
   host = config.get("DBCRED", 'host')
   port = config.get("DBCRED", 'port')
   user = config.get("DBCRED", 'user')
   passwd = config.get("DBCRED", 'pass')
   url = host + ":" + port + "/" + db
   print(url)

   db_df = sparksess.read.format("jdbc").option("url",url) \
      .option("dbtable", tbl) \
      .option("user", user).option("password", passwd) \
      .option("driver", driver) \
      .option("lowerBound", lowerbound) \
      .option("upperBound", upperbound) \
      .option("numPartitions", numpart) \
      .option("partitionColumn", partcol) \
      .load()
   db_df.show(1)
   return db_df

def writeRDBMSData(df, propfile, db, tbl, mode):
   config = configparser.ConfigParser()
   config.read(propfile)
   driver = config.get("DBCRED", 'driver')
   host = config.get("DBCRED", 'host')
   port = config.get("DBCRED", 'port')
   user = config.get("DBCRED", 'user')
   passwd = config.get("DBCRED", 'pass')
   url = host + ":" + port + "/" + db
   url1 = url + "?user=" + user + "&password=" + passwd
   df.write.jdbc(url=url1, table=tbl, mode=mode)

def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("AWS RDS Read/Write") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
      .config("spark.jars", "file:///home/hduser/install/postgresql-42.2.19.jar") \
      .enableHiveSupport()\
      .getOrCreate()
   #.config("spark.driver.extraClasspath", "file:///home/hduser/install/postgresql-42.2.19.jar") \
#      .config("spark.executor.extraClasspath", "file:///home/hduser/install/postgresql-42.2.19.jar") \
#      .config("spark.driver.extraClassPath", "file:///home/hduser/install/aws-java-sdk-1.12.281.jar") \

   drugs_query = """(select * from healthcare.drugs where loaddt=current_date) query """
   drugs_df=getRdbmsPartData('/home/hduser/install/aws/connection_rds.prop',spark,'dev',drugs_query,"uniqueid",397,232090,4)
   #--split-by "uniqueid" min id 1 max id 1000 -m 4
   #print(drugs_df.toDF().rdd.getNumPartitions())
   drugs_df.show()
   drugs_df.write.mode("overwrite").partitionBy("loaddt").saveAsTable("sparkdb.drugs_part")
   spark.read.table("sparkdb.drugs_part").show(5,False)
   print("Spark AWS RDS Read using partition feature and write to hive part table is Completed Successfully")

if __name__ == '__main__':
 main()