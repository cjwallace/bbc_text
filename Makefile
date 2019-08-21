.PHONY: data hdfs test

data:
	scripts/fetch_data.sh

hdfs:
	scripts/hdfs_data.sh

test:
	python3 -m pytest -p no:warnings
