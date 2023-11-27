# Running on Spark Cluster

## IdealizedPageRank

spark-submit --class IdealizedPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/idealizedOutput

##### Output All

hadoop fs -cat /PA3/idealizedOutput/*

##### Get Top 10

hadoop fs -head /PA3/idealizedOutput/part-00000

## TaxationPageRank

spark-submit --class TaxationPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/taxationOutput

##### Output All

hadoop fs -cat /PA3/taxationOutput/*

##### Get Top 10

hadoop fs -head /PA3/taxationOutput/part-00000

## WikiBomb

spark-submit --class WikiBomb --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/wikibombOutput

##### Output All

hadoop fs -cat /PA3/wikibombOutput/*

##### Get Top 10

hadoop fs -head /PA3/wikibombOutput/part-00000

## Colorado State University
## CS435 Big Data PA3