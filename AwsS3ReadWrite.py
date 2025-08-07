#prerequisites
#1. If you have the dependency jars maintained in a location in hdfs/lfs then use --jars to refer it explicitly
#spark-submit --jars "file:///home/hduser/install/aws/s3_jars/*" s3_we39.py
#1. or keep the above jars into /usr/local/spark/jars and run the job as given below
#spark-submit s3_we39.py
#2. Ensure to add the access and secret in the code or in aws configure or in core-site.xml or in a jceks file and try

#Drawback in this program is the access key and secret key is hardcoded and anyone can see it and it is non modifyable without touching the code.
#To avoid these drawback of hardcoding some sensitive info or variable informations -
# pass as a set at the environment level, parameters/arguments for passing location etc.,, (best way) keep these sensitive info in encrypted files eg. jceks, create a property file and give a access control to the file only for the
# user who is authorized and use in the program

from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("AWS S3 Read/Write") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
      .enableHiveSupport()\
      .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "#####################")
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "########################+3###########")
   sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
   sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
   sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
   sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

   print("Use Spark Application to Read csv data from cloud S3 and get a DF created with the s3 data in the on prem, "
         "convert csv to json in the on prem DF and store the json into new cloud s3 location")
   print("Hive to S3 to hive starts here")
   s3_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("s3a://com.iz.test1/data/cust.csv").toDF("id","name","age")
   s3_df.show()
   print("S3 Read Completed Successfully")
   s3_df.write.mode("overwrite").partitionBy("age").saveAsTable("default.cust_info_s3")
   print("S3 to hive table load Completed Successfully")

   print("Hive to S3 usecase starts here")
   s3_df=spark.read.table("default.cust_info_s3")
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   print(curts)
   s3_df.repartition(2).write.json("s3a://com.iz.test1/output_wd28/cust_json_"+curts)
   print("S3 Write Completed Successfully")

main()