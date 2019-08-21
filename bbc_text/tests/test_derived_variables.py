import pytest
import os
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

import bbc_text.derived_variables as dv


@pytest.fixture
def word_count_df(spark):
    pdf = pd.DataFrame({
        'id': [0, 1, 2, 3],
        'text': ['Hello',
                 'Hello there, how are you?',
                 '   Many spaces     ',
                 '']
    })
    df = spark.createDataFrame(pdf)
    df = df.withColumn('word_count', dv.word_count(df, 'text'))
    return df


class TestWordCount:
    def test_word_count_returns_1_for_single_word(self, word_count_df):
        assert word_count_df.collect()[0]['word_count'] == 1
        
    def test_word_count_returns_count_of_many_words(self, word_count_df):
        assert word_count_df.collect()[1]['word_count'] == 5
    
    def test_word_count_ignores_spaces(self, word_count_df):
        assert word_count_df.collect()[2]['word_count'] == 2

#    def test_word_count_returns_zero_for_empty_string(self, word_count_df):
#        assert word_count_df.collect()[3]['word_count'] == 0
    
    def test_word_count_can_be_used_in_select(self, spark):
        df = spark.createDataFrame(pd.DataFrame({
            'id': [0],
            'text': ['Hello there, how are you?']
        }))
        wc = df.select(dv.word_count(df, 'text').alias('word_count'))
        assert wc.collect()[0]['word_count'] == 5