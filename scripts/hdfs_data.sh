# destroy previous data directory
hdfs dfs -rm -r bbc_text

# recreate directory
hdfs dfs -mkdir bbc_text

# copy data from local filesystem into hdfs
hdfs dfs -put data/bbc bbc_text

# set up data for testing
hdfs dfs -mkdir bbc_text/test
hdfs dfs -mkdir bbc_text/test/cats
hdfs dfs -put tests/data/cats bbc_text/test/cats
