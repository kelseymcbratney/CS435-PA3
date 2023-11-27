import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import java.text.DecimalFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TaxationPageRank {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: TaxationPageRank <inputLinksFile> <inputTitlesFile> <outputDirectory>");
            System.exit(1);
        }

        String linksFile = args[0];
        String titlesFile = args[1];
        String outputDir = args[2];

        // Initialize Spark
        SparkConf conf = new SparkConf()
                .setAppName("WikiBomb");
                //.setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // Load the data
            JavaRDD<String> lines;
            lines = sc.textFile(linksFile);

            // Parse the data to create the transition matrix
            JavaPairRDD<Integer, List<Integer>> transitionMatrix;
            transitionMatrix = lines.mapToPair(line -> {
                String[] parts = line.split(":");
                int from = Integer.parseInt(parts[0].trim());
                String[] toLinks = parts[1].trim().split(" ");
                List<Integer> toPages = Arrays.stream(toLinks)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                return new Tuple2<>(from, toPages);
            });

            // Define the initial vector
            int numPages = (int) transitionMatrix.keys().distinct().count();
            List<Double> initialVector = new ArrayList<>();
            for (int i = 0; i < numPages; i++) {
                initialVector.add(1.0 / numPages);
            }

            // Define the teleportation probability
            double beta = 0.85;

            // Define the number of iterations
            int numIterations = 25;

            List<Double> currentVector = initialVector;

            // Perform Idealized PageRank iterations
            for (int iteration = 0; iteration < numIterations; iteration++) {
                List<Double> finalCurrentVector = currentVector;
                JavaPairRDD<Integer, Double> nextVector = transitionMatrix.flatMapToPair(s -> {
                    int from = s._1();
                    List<Integer> toPages = s._2();
                    double rank;
                    if (from >= 1 && from <= finalCurrentVector.size()) {
                        rank = finalCurrentVector.get(from - 1);
                    } else {
                        rank = 0.0;
                    }
                    int linkCount = toPages.size();
                    List<Tuple2<Integer, Double>> contribs = new ArrayList<>();
                    for (Integer toPage : toPages) {
                        contribs.add(new Tuple2<>(toPage, rank / linkCount));
                    }
                    return contribs.iterator();
                }).reduceByKey((Function2<Double, Double, Double>) Double::sum).mapValues(rank -> (rank * beta) + (1 - beta) * (1 / numPages));

                currentVector = nextVector.values().collect();
            }


            // Sort the pages by PageRank in descending order
            JavaPairRDD<Double, Long> sortedPageRanks;
            sortedPageRanks = sc.parallelize(currentVector)
                    .zipWithIndex()
                    .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()))
                    .sortByKey(false);

            // Load the page titles
            JavaRDD<String> titles = sc.textFile(titlesFile);

            // Join the page titles with the final PageRank scores
            JavaPairRDD<Long, String> pageTitles = titles.zipWithIndex().mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()));

            // Collect page titles into a map
            Map<Long, String> pageTitleMap = pageTitles.collectAsMap();

            // Create a broadcast variable for the page titles map
            Broadcast<Map<Long, String>> pageTitleMapBroadcast = sc.broadcast(pageTitleMap);

            // Use the broadcast variable to retrieve page titles
            JavaPairRDD<String, String> joinedData = sortedPageRanks.mapToPair(new PairFunction<Tuple2<Double, Long>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<Double, Long> tuple) {
                    Map<Long, String> titlesMap = pageTitleMapBroadcast.getValue();
                    String pageTitle = titlesMap.get(tuple._2());

                    // Format the double value to avoid scientific notation
                    DecimalFormat decimalFormat = new DecimalFormat("#.####################");
                    String formattedValue = decimalFormat.format(tuple._1());

                    return new Tuple2<>(pageTitle, formattedValue);
                }
            });

            // Sorted list of Wikipedia pages based on their PageRank value to a text file
            joinedData.saveAsTextFile(outputDir);

            // Stop Spark
            sc.stop();
        }
    }
}
