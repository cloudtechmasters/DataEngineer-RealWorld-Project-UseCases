from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars","C:\\Users\\muppa\\Downloads\\snowflake-jdbc-3.13.30.jar,C:\\Users\\muppa\\Downloads\\spark-snowflake_2.12-2.11.3-spark_3.1.jar")\
    .config("fs.s3a.access.key", "") \
    .config("fs.s3a.secret.key", "") \
    .config("fs.s3a.endpoint", "s3.amazonaws.com") \
    .master("local").appName("PySpark_S3_test").getOrCreate()
sfparams = {
    "sfURL": "hb71718.ap-south-1.aws.snowflakecomputing.com",
    "sfUser": "*****",
    "sfAccount": "hb71718",
    "sfPassword": "*******",
    "sfDatabase": "SNOWFLAKE_LEARNING",
    "sfSchema": "employee",
    "sfWarehouse": "COMPUTE_WH",
    "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
}
customerDf=spark.read.format("net.snowflake.spark.snowflake").options(**sfparams).option("dbtable",
                                                                                         "SNOWFLAKE_LEARNING.employee.customer").load()
customerDf.show()
