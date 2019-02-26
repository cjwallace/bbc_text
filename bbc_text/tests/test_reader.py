import pytest
import os
import bbc_text.reader as sut

from pyspark.sql import SparkSession, DataFrame
from pyspark.rdd import RDD


ROOT_DIR = os.getcwd()

@pytest.fixture
def articles_rdd(spark_context):
    rdd = sut.read_articles(
        spark_context,
        "file:///{}/bbc_text/tests/data".format(ROOT_DIR),
        "cats"
    )
    return rdd

@pytest.fixture
def articles_df(spark, articles_rdd):
    df = sut.create_article_df(
        spark,
        articles_rdd,
        'cats'
    )
    return df
    

class TestReadArticles:
    def test_rdd_returned(self, articles_rdd):
        """Return type should be an rdd."""
        assert isinstance(articles_rdd, RDD)
    
    def test_all_read(self, articles_rdd):
        """All articles in the directory should become rows"""
        assert articles_rdd.count() == 3

    def test_text_in_rdd(self, articles_rdd):
        """First article text should be in the rdd"""
        text = articles_rdd.first()[1]
        assert text == "This is an article about a cat."


class TestCreateArticleDf:
    def test_df_is_returned(self, articles_df):
        """Test that a DataFrame is returned."""
        assert isinstance(articles_df, DataFrame)
        
    def test_df_is_returned(self, articles_df, articles_rdd):
        """DataFrame should have same number of rows as rdd."""
        assert articles_df.count() == articles_rdd.count()
        
    def test_df_has_three_colums(self, articles_df):
        """DataFrame should have three columns: filename, text and category."""
        assert len(articles_df.columns) == 3
        assert set(articles_df.columns) == {'filename', 'text', 'category'}
        
    def test_df_category_is_cats(self, articles_df):
        """The contents of the dataframe category column should be a string
           of the category name, 'cats'."""
        categories = articles_df.select('category').distinct()
        assert categories.count() == 1
        assert categories.collect()[0]['category'] == 'cats'
        
    def test_df_text_column_has_text(self, articles_df, articles_rdd):
        """DataFrame should have the same text as the it is passed."""
        rdd_text = articles_rdd.first()[1]
        df_text = articles_df.first()['text']
        assert df_text == rdd_text