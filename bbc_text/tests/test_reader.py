import pytest
import os

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

import bbc_text.reader as reader


ROOT_DIR = os.getcwd()


@pytest.fixture
def articles_rdd(spark_context):
    rdd = reader.read_articles(
        spark_context,
        "file:///{}/bbc_text/tests/data".format(ROOT_DIR),
        "cats"
    )
    return rdd


@pytest.fixture
def single_category_df(spark, articles_rdd):
    df = reader.create_single_category_df(
        spark,
        articles_rdd,
        'cats'
    )
    return df


@pytest.fixture
def articles_df(spark, articles_rdd):
    df = reader.create_articles_df(
        spark,
        "file:///{}/bbc_text/tests/data".format(ROOT_DIR),
        ['cats', 'dogs']
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


class TestCreateSingleCategoryDf:
    def test_df_is_returned(self, single_category_df):
        """Test that a DataFrame is returned."""
        assert isinstance(single_category_df, DataFrame)

    def test_df_is_returned(self, single_category_df, articles_rdd):
        """DataFrame should have same number of rows as rdd."""
        assert single_category_df.count() == articles_rdd.count()

    def test_df_has_three_colums(self, single_category_df):
        """DataFrame should have three columns: filename, text and category."""
        assert len(single_category_df.columns) == 3
        assert set(single_category_df.columns) == {'filename', 'text', 'category'}

    def test_df_category_is_cats(self, single_category_df):
        """The contents of the dataframe category column should be a string
           of the category name, 'cats'."""
        categories = single_category_df.select('category').distinct()
        assert categories.count() == 1
        assert categories.collect()[0]['category'] == 'cats'

    def test_df_text_column_has_text(self, single_category_df, articles_rdd):
        """DataFrame should have the same text as the it is passed."""
        rdd_text = articles_rdd.first()[1]
        df_text = single_category_df.first()['text']
        assert df_text == rdd_text


class TestCreateArticleDf:
    def test_df_is_returned(self, articles_df):
        assert isinstance(articles_df, DataFrame)

    def test_both_listed_categories_present(self, articles_df):
        assert set(articles_df.toPandas()['category'].unique()) == \
            {'cats', 'dogs'}

    def test_all_articles_found(self, articles_df):
        assert articles_df.count() == 5