from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def spark_yarn_session():
    """Return a SparkSession on YARN."""
    spark = (
        SparkSession
        .builder
        .master('yarn')
        .appName('bbc_text')
        .getOrCreate()
    )
    return spark
  
def spark_local_session():
    """Return a local SparkSession."""
    spark = (
        SparkSession
        .builder
        .master('local')
        .appName('bbc_text')
        .getOrCreate()
    )
    return spark