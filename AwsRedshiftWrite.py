def main():
   from pyspark.sql import SparkSession
   print("source file passed as argument is ")
   spark = SparkSession.builder\
      .appName("AWS Redshift Read/Write") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
      .enableHiveSupport()\
      .getOrCreate()
   spark.sparkContext.addPyFile("file:///home/hduser/install/hadoop-aws-2.7.3.jar")
   sc=spark.sparkContext
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "###############################")
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "####################+######################")
   sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
   sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
   sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

   dfpatients = spark.read.option("inferschema", True).option("header", True).\
      csv("file:///home/hduser/install/aws/patients.csv")

   dfpatients.write.format("io.github.spark_redshift_community.spark.redshift").\
      option("url","jdbc:redshift://redshift-cluster-1.cn3haetn4qhf.us-east-1.redshift.amazonaws.com:5439/sample_data_dev?user=inceptezdb&password=Inside123$")\
      .option("forward_spark_s3_credentials", True)\
      .option("dbtable", "patients2").option("tempdir", "s3a://com.iz.datalake/redshift_tempdir/").mode("append").save()
   print("Data to RedShift")
main()
