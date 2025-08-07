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
      #connector/driver jars will be referred in the order of 1. spark/jars (priority3) 2. expects the argument --jars (priority2) 3. spark.jars config (priority1)
      #1st priority for the properties mentioned in the code, 2nd priority for the arguments/property files that we use and 3rd for the site.xml or environment level properties
      #Dont hardcode the access/secret in the code, rather store in a plain text file or better store it in a encrypted file for
      # eg. jceks (java cryptography encrypted key store) and finally define chmod 400 to the file

   # hadoop credential create aws_access -value ##################################### -provider jceks://hdfs@localhost:54310/user/hduser/s3wd28.jceks
   # hadoop credential create aws_secret -value ############################################## -provider jceks://hdfs@localhost:54310/user/hduser/s3wd28.jceks
   # hadoop credential list -provider jceks://hdfs@localhost:54310/user/hduser/s3wd28.jceks
   # hadoop fs -chmod 400 /user/hduser/s3wd28.jceks
   sc=spark.sparkContext
   sc.setLogLevel("ERROR")
   credential_path="jceks://hdfs@localhost:54310/user/hduser/s3wd28.jceks"
   conf=sc._jsc.hadoopConfiguration()
   conf.set('hadoop.security.credential.provider.path', credential_path)

   accesskey = conf.getPassword('aws_access')
   #decoding access key
   accesskey_str = ''
   for i in range(len(accesskey)):
      accesskey_str = accesskey_str + str(accesskey[i])

   #decoding secret key
   secretkey = conf.getPassword('aws_secret')
   secretkey_str = ''
   for i in range(secretkey.__len__()):
      secretkey_str = secretkey_str + str(secretkey.__getitem__(i))

   print(accesskey_str)
   print(secretkey_str)

   sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", accesskey_str)
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", secretkey_str)
   sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
   sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
   sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
   sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

   print("Use Spark Application to Read csv data from cloud S3 and get a DF created with the s3 data in the on prem, "
         "convert csv to json in the on prem DF and store the json into new cloud s3 location")
   s3_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("s3a://com.iz.test1/data/cust.csv").toDF("id","name","age")
   s3_df.show()
   print("S3 Read Completed Successfully")
   curts = spark.createDataFrame([1], IntegerType()).withColumn("current_timestamp", current_timestamp()).select(date_format(col("current_timestamp"), "yyyyMMddHHmmSS")).take(1)[0][0]
   print(curts)
   s3_df.repartition(2).write.json("s3a://com.iz.test1/output_wd28/cust_json_jceks_"+curts)
   print("S3 Write Completed Successfully")

main()
