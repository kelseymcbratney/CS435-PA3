** Running on Spark Cluster **

IdealizedPageRank

spark-submit --class IdealizedPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/output

TaxationPageRank

spark-submit --class TaxationPageRank --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/output

WikiBomb

spark-submit --class WikiBomb --master spark://salem:30160 PageRank.jar /PA3/links-simple-sorted.txt /PA3/titles-sorted.txt /PA3/output
