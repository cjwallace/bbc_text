# BBC News

A short text classification application.
Rather than chasing a good solution to the problem itself, we focus on good
engineering practice.


## Data 

Dataset source: http://mlg.ucd.ie/datasets/bbc.html

To fetch and unzip the data, run the command:
(_warning_: this is a destructive act - consult the source for the local
directories that will be re-written)
```bash
make data
```

To place the data in hdfs:
(_warning_: this is a destructive act - consult the source for the HDFS
directories that will be re-written)

```bash
make hdfs
```


From the source:

>Consists of 2225 documents from the BBC news website corresponding to stories
>in five topical areas from 2004-2005.
>Natural Classes: 5 (business, entertainment, politics, sport, tech)

The data set was curated for the publication 
```
D. Greene and P. Cunningham.
"Practical Solutions to the Problem of Diagonal Dominance in Kernel Document
Clustering", Proc. ICML 2006.
```
All rights, including copyright, in the content of the original articles are owned by 
the BBC.