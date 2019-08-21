# # Creating a derived word count variable

# ## Imports

from bbc_text.sessions import spark_local_session
from bbc_text.reader import create_articles_df
from bbc_text.derived_variables import word_count


# ## Environment variables

DATA_DIR = 'file:///home/cdsw/data/bbc/'


# ## Spark session

spark = spark_local_session()


# ## Read data

categories = ['business', 'entertainment', 'politics', 'sport', 'tech']
articles_df = create_articles_df(spark, DATA_DIR, categories)


# ## Create a new column

articles_df.withColumn("word_count", word_count(articles_df, 'text')).show()