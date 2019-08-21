import pyspark.sql.types as typ
import pyspark.sql.functions as F

from functools import reduce
from pyspark.sql import DataFrame

def read_articles(sc, datadir, category):
    """
    Takes a SparkContext, data directory name, and subfolder (category)
    name, and returns an RDD of (filename, text) pairs.
    Requires that articles are stored in HDFS in individual .txt files in
    subdirectories with the category name.
    """
    path = datadir + '/' + category
    articles_rdd = sc.wholeTextFiles(datadir + '/' + category)
    return articles_rdd


def create_single_category_df(spark, articles_rdd, category):
    """
    Create a DataFrame, given a RDD of (filename, text) pairs.
    """
    
    schema = typ.StructType(
        [typ.StructField('filename', typ.StringType(), False),
         typ.StructField('text', typ.StringType(), True)]
    )
    
    df = (
        spark
        .createDataFrame(articles_rdd, schema)
        .withColumn('category', F.lit(category))
    )
    
    return df


def create_articles_df(spark, datadir, categories):
    """
    Create a DataFrame containing the article text and category
    for all listed categories.
    """
    sc = spark.sparkContext
    article_dfs = [
        create_single_category_df(
            spark,
            read_articles(sc, datadir, category),
            category
        )
        for category in categories
    ]
    
    all_articles_df = reduce(DataFrame.unionAll, article_dfs)
    
    return all_articles_df