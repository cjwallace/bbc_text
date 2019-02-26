# Remove and recreate the data directory
rm -rf data
mkdir data

# Fetch raw text data
curl http://mlg.ucd.ie/files/datasets/bbc-fulltext.zip > data/bbc-fulltext.zip

# Unzip (the zip contains multiple files, which thus must be unzipped
# before transfer to hdfs)
unzip data/bbc-fulltext.zip -d data