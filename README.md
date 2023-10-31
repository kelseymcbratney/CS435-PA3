# Running on Spark Cluster

### IdealizedPageRank

spark-submit --class IdealizedPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/idealizedOutput

### TaxationPageRank

spark-submit --class TaxationPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/taxationOutput

### WikiBomb

spark-submit --class WikiBomb --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/wikibombOutput

## Colorado State University
## CS435 Big Data PA3