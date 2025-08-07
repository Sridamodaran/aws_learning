#https://github.com/ramitsurana/redshift-scripts/blob/master/UPDATE-INSERT/staging-table-row-operations.sql
from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   #src_data =srcpath #"file:/home/hduser/hive/data/custsmodified_malformed"
   print("source file passed as argument is ")
   #print(src_data)
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("AWS S3 Read/Write") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
      .config("spark.executor.extraClasspath","file:///home/hduser/install/jets3t-0.9.4.jar") \
      .config("spark.executor.extraClassPath","file:///home/hduser/install/aws-java-sdk-1.12.281.jar") \
      .config("spark.driver.extraClasspath", "file:///home/hduser/install/jets3t-0.9.4.jar")      \
      .config("spark.driver.extraClasspath","file:///home/hduser/install/aws-java-sdk-1.12.281.jar") \
      .config("spark.executor.extraClassPath", "file:///home/hduser/aws/jars/redshift-jdbc42-2.1.0.9.jar") \
      .config("spark.driver.extraClasspath", "file:///home/hduser/aws/jars/redshift-jdbc42-2.1.0.9.jar") \
      .config("spark.executor.extraClassPath", "file:///home/hduser/aws/jars/spark-redshift_2.12-5.0.3.jar") \
      .config("spark.driver.extraClasspath", "file:///home/hduser/aws/jars/spark-redshift_2.12-5.0.3.jar") \
      .config("spark.executor.extraClassPath", "file:///home/hduser/aws/jars/minimal-json-0.9.4.jar") \
      .config("spark.driver.extraClasspath", "file:///home/hduser/aws/jars/minimal-json-0.9.4.jar") \
      .config("spark.executor.extraClassPath", "file:///home/hduser/install/hadoop-aws-2.7.3.jar") \
      .config("spark.driver.extraClasspath", "file:///home/hduser/install/hadoop-aws-2.7.3.jar") \
      .enableHiveSupport()\
      .getOrCreate()
   spark.sparkContext.addPyFile("file:///home/hduser/install/hadoop-aws-2.7.3.jar")
#      .config("spark.executor.extraClassPath", "file:///home/hduser/aws/jars/hadoop-aws-2.7.1.jar") \
#      .config("spark.driver.extraClasspath", "file:///home/hduser/aws/jars/hadoop-aws-2.7.1.jar") \
   """spark.sparkContext.addPyFile("file:///home/hduser/install/jets3t-0.9.4.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws-java-sdk-1.12.281.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/hadoop-aws-2.7.3.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws/jars/minimal-json-0.9.4.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws/jars/redshift-jdbc42-2.1.0.9.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws/jars/scala-library-2.12.11.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws/jars/slf4j-api-1.7.5.jar")
   spark.sparkContext.addPyFile("file:///home/hduser/install/aws/jars/spark-redshift_2.12-5.0.3.jar")"""

#.config("spark.jars", "file:///home/hduser/install/hadoop-aws-2.7.3.jar") \
   #
      #connector/driver jars will be referred in the order of 1. spark/jars (priority3) 2. expects the argument --jars (priority2) 3. spark.jars config (priority1)
   # Set the logger level to error
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "AKIA3YF5CC3LQDFLDUMU")
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "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")
   #sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
   sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
   sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
   sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

   s3_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("s3a://com.iz.test1/data/cust.csv").toDF("id","name","age")
   s3_df.show()
   print("Spark App1 Completed Successfully")
   dfpatients = spark.read.format("io.github.spark_redshift_community.spark.redshift")\
      .option("url","jdbc:redshift://redshift-cluster-1.cn3haetn4qhf.us-east-1.redshift.amazonaws.com:5439/sample_data_dev?user=inceptezdb&password=Inside123$")\
      .option("forward_spark_s3_credentials", True)\
      .option("dbtable", "tickit.venue").option("tempdir", "s3a://com.inceptez.shellbucket/hadoop/").load()
   print("Data from RedShift")
   dfpatients.cache()
   dfpatients.show(5,False)
   #dfpatients.na.drop.createOrReplaceTempView("redshiftpatients")
main()
#.option("jdbcdriver", "org.postgresql.Driver")\