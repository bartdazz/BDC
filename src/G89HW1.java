import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class G89HW1 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Disable Spark and Akka logs
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create Spark configuration with master URL
        SparkConf conf = new SparkConf().setAppName("G89HW1");
        // I think we don't need setMaster since we'll set it from intellij
        // .setMaster("local[*]"); // Use all CPU cores

        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        // sc.setLogLevel("WARN"); // same setting as the example

        // Reading the input
        int L = Integer.parseInt(args[1]); // number of partitions of the RDD
        int K = Integer.parseInt(args[2]); // number of clusters
        int M = Integer.parseInt(args[3]); // number of iterations

        // read the text file in input
        JavaRDD<String> points = sc.textFile(args[0]).cache().repartition(L);

        // .repartition is used to tell spark to divide the input in L partitions
        // inside the RDD

        // SETTING GLOBAL VARIABLES
        long numpoints = points.count();

        // build a key-value pairs RDD
        JavaPairRDD<Vector, String> inputPoints;

        // the following chunk of code will create a key-value pairs RDD
        inputPoints = points
                .flatMapToPair((pointClass) -> {
                    // splitting the String representing the point and it's class
                    String[] tokens = pointClass.split(",");

                    // getting the length of the splitted array
                    Integer len = tokens.length;

                    // Extracting the sublist regarding the point's coordinates
                    String[] pointString = Arrays.copyOfRange(tokens, 0, len - 1);

                    // Transforming the coordinates into doubles
                    // COPIED FROM CHATGPT : UNDERSTAND BETTER !!!
                    /*
                     * could also do something like :
                     * for (int i = 0; i < sarray.length; i++) {
                     * values[i] = Double.parseDouble(sarray[i]);
                     * }
                     */
                    double[] pointDouble = Arrays
                            .stream(pointString)
                            .mapToDouble(Double::parseDouble)
                            .toArray();

                    // Creating the vector
                    Vector point = Vectors.dense(pointDouble);

                    // Extracting the demographic class
                    String demoClass = tokens[len - 1];

                    // Create the ArrayList that will contain the key-value pairs
                    ArrayList<Tuple2<Vector, String>> pairs = new ArrayList<>();

                    // Create the Tuple2 of a single pair point-class
                    Tuple2<Vector, String> singlePair = new Tuple2<Vector, String>(point, demoClass);

                    // Add the pair to the ArrayList pairs
                    pairs.add(singlePair);

                    // return an iterator over the pairs as the result of this function
                    return pairs.iterator();
                });

        // Doing an action on the RDD to check if everything is ok
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // ATTENTION
        // 
        // Maybe this isn't needed !!
        long lenPoints = inputPoints.count();

        // First Map Reduce part to compute the number of elements belonging to each
        // class
        JavaPairRDD<String, Integer> classItems; // build a new RDD
        classItems = inputPoints
                // element is a key-value pair
                // with this map we want to set as key the demographic class
                // so the second element of the Tuple2
                // and as value we set 1 that then will be summed up in
                // the reduceByKey phase
                .mapToPair((element) -> new Tuple2<>(element._2(), 1))
                .reduceByKey((x, y) -> x + y); // this sums all the 1 for each class

        // The function collect takes al the data stored in the RDD and puts
        // it into a list; the RDD is sufficiently small to allow us to do so
        List<Tuple2<String, Integer>> classCounts = classItems.collect();

        // Print the number of elements in each class
        // Each element in the for loop is a Tuple2
        int classA = 0;
        int classB = 0;
        for (Tuple2<String, Integer> classCount : classCounts) {
            String className = classCount._1(); // The class name (key)
            Integer count = classCount._2(); // The count of elements in that class (value)
            if (Objects.equals(className, "A")) { // object equals corresponds to ==
                classA = count;
            } else {
                classB = count;
            }
        }
        // print command-line arguments
        /*
         * print all the output here otherwise spark prints its log info in the middle
         */
        System.out.println("Input file = " + args[0]
                + ", L = " + L
                + ", K = " + K
                + ", M = " + M);

        System.out.println("N = " + numpoints
                + ", NA = " + classA
                + ", NB = " + classB);

        KMeansModel clusters = KMeans.train(inputPoints.map(Tuple2::_1).rdd(), K, M);
        // get centers
        Vector[] centers = clusters.clusterCenters();

        // Stop SparkContext at the end
        sc.close();
    }
}

class mymethods {
    /*
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * MR COMPUTE STANDARD OBJECTIVE
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * Implementation:
     *
     * ----- ROUND 1 ----
     * Map each (Point, demographic class) pair to (Point, d_i) where d_i is the
     * distance between the point and the i-th centroid computed by the LLoyd's
     * algorithm. This produces a larger RDD
     * Then we group elements by key which is the point (Shuffle phase) and the
     * reduce phase consists in taking the minimum distance for each point
     *
     * ---- ROUND 2 ----
     * Map each (Point, smaller distance) to (0, smaller distance)
     * Shuflle and then in the reduce phase sum all the distances
     */
    public double MRComputeStandardObjective(JavaPairRDD<Vector, String> rdd, Vector[] centroids) {
        long numPoints = rdd.count();
        JavaPairRDD<Integer, Double> StandardObjective;
        ArrayList<Tuple2<Vector, Double>> pointDistances = new ArrayList<>();

        // Round 1
        StandardObjective = rdd.flatMapToPair((pair) -> {
            Vector point = pair._1();
            for (Vector center : centroids) {
                // compute the distance between the selected center and the point
                double distance = Vectors.sqdist(point, center);
                pointDistances.add(new Tuple2<Vector, Double>(point, distance));
            }
            return pointDistances.iterator();
        })
                .reduceByKey((x, y) -> Math.min(x, y))

                // Round 2
                .mapToPair((element) -> new Tuple2<>(0, element._2()))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<Integer, Double>> StandObj = StandardObjective.collect();
        double result = StandObj.get(0)._2();
        result = result / numPoints;
        return result;
    }
    
    // MRComputeFairObjective
    // MRPrintStatistics


}
