import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import spire.random.Seed;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

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
        // I think we don't need setMaster since we set it from intellij
        // .setMaster("local[*]"); // Use all CPU cores

        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN"); // same setting as the example

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

                    // Extracting the sublist regarding the point's coordinates
                    String[] pointString = Arrays.copyOfRange(tokens, 0, 2);

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
                    String demoClass = tokens[2];

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
        // long lenPoints = inputPoints.count();

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
        // in classCount._1() there is the class name (key)
        // in classCount._2() there is the count of elements in that class (value)
        int classA = 0, classB = 0;
        for (Tuple2<String, Integer> classCount : classCounts) {
            if (Objects.equals(classCount._1(), "A")) { // object equals corresponds to ==
                classA = classCount._2();
            } else {
                classB = classCount._2();
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

        // initialize and train the model
        KMeansModel clusters = KMeans.train(inputPoints.map(Tuple2::_1).rdd(), K, M,
                "k-means||", // Initialization mode ("random" or "k-means||")
                new Random(42).nextLong()); // Set a specific seed)
        // get centers
        Vector[] centers = clusters.clusterCenters();

        // result of MRComputeStandardObjective
        System.out.println("Delta(U,C) = " + mymethods.MRComputeStandardObjective(inputPoints, centers));
        // result of MRComputeFairObjective
        System.out.println("Phi(A,B,C) = " + mymethods.MRComputeFairObjective(inputPoints, centers));

        // check centers
        for (Vector c : centers) {
            System.out.println(c);
        }

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
     * ---- ROUND 1 ----
     * Map each (Point, demographic class) pair to (Point, d_i) where d_i is the
     * distance between the point and the i-th centroid computed by the LLoyd's
     * algorithm. This produces a larger RDD
     * Then we group elements by key which is the point (Shuffle phase) and the
     * reduce phase consists in taking the minimum distance for each point
     *
     * ---- ROUND 2 ----
     * Map each (Point, smaller distance) to (0, smaller distance)
     * Shuffle and then in the reduce phase sum all the distances
    */
    public static double MRComputeStandardObjective(JavaPairRDD<Vector, String> rdd, Vector[] centroids) {
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
                // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
                // IMPROVE THIS REDUCE PHASE WITH THE RDD PARTITIONS
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<Integer, Double>> StandObj = StandardObjective.collect();

        return StandObj.get(0)._2() / numPoints;
    }

    /*
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * MR COMPUTE FAIR OBJECTIVE
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * Implementation:
     *
     * ---- ROUND 1 ----
     * Map each (Point, demographic class) pair to (Point, [demographic class, d_i])
     * where d_i is the squared distance between the point and the i-th centroid
     * computed by the LLoyd's algorithm. This produces a larger RDD.
     * Then we group elements by key which is the point (Shuffle phase) and the
     * reduce phase consists in taking the minimum distance for each point
     *
     * ---- ROUND 2 ----
     * In the map phase set as key the demographic class, discarding the point and
     * set as value the minimum distance of the point from the centroids.
     * Then group elements by key and sum all the distances.
     */
    public static double MRComputeFairObjective(JavaPairRDD<Vector, String> rdd, Vector[] centroids) {
        // Map<Tuple2<Vector, String>, Long> valueCount = rdd.countByValue();
        // %%%%%%%%%%%% Code to compute NA, NB : it can be improved
        JavaPairRDD<String, Integer> classItems; // build a new RDD
        classItems = rdd
                // element is a key-value pair
                // with this map we want to set as key the demographic class
                // so the second element of the Tuple2
                // and as value we set 1 that then will be summed up in
                // the reduceByKey phase
                .mapToPair((element) -> new Tuple2<>(element._2(), 1))
                .reduceByKey((x, y) -> x + y); // this sums all the 1 for each class
                // now in the rdd there are only two Tuple2<>( , )
                // with A and B as a key and NA and NB as value

        // The function collect takes all the data stored in the RDD and puts
        // it into a list; the RDD is sufficiently small to allow us to do so
        List<Tuple2<String, Integer>> classCounts = classItems.collect();

        // Print the number of elements in each class
        // Each element in the for loop is a Tuple2
        // in classCount._1() there is the class name (key)
        // in classCount._2() there is the count of elements in that class (value)
        int classA = 0, classB = 0;
        for (Tuple2<String, Integer> classCount : classCounts) {
            if (Objects.equals(classCount._1(), "A")) { // object equals corresponds to ==
                classA = classCount._2();
            } else {
                classB = classCount._2();
            }
        }
        // %%%%%%%%%%%%%% End of code to compute NA, NB

        // Round 1
        long numPoints = rdd.count();
        JavaPairRDD<String, Double> FairObjective; // output RDD
        ArrayList<Tuple2<Vector, Tuple2<String, Double>>> pointDistancesClass = new ArrayList<>();

        // Round 1
        FairObjective = rdd.flatMapToPair((pair) -> {
            Vector point = pair._1(); // point
            String demoClass = pair._2(); // demographic class
            for (Vector center : centroids) {
                // compute the distance between the selected center and the point
                double distance = Vectors.sqdist(point, center);
                // Tuple containing the demographic class and the distance
                Tuple2<String, Double> classDistance = new Tuple2<String, Double>(demoClass, distance);
                pointDistancesClass.add(new Tuple2<Vector, Tuple2<String, Double>>(point, classDistance));
            }
            return pointDistancesClass.iterator();
        })
                .reduceByKey((x, y) -> {
                    // return the tuple with the smaller distance
                    Double distanceX = x._2();
                    Double distanceY = y._2();
                    if (distanceX < distanceY) {
                        return x;
                    } else {
                        return y;
                    }
                })
                // Round 2
                // in element._2()._1() there is the class, A or B
                // in element._2()._2() there is the distance
                .mapToPair((element) -> new Tuple2<>(element._2()._1(), element._2()._2()))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Double>> StandObj = FairObjective.collect();
        double fairA = 0.0, fairB = 0.0;
        for (Tuple2<String, Double> classSum : StandObj) {
            if (classSum._1().equals("A")) {
                fairA = classSum._2() / classA;
            } else {
                fairB = classSum._2() / classB;
            }
        }
        return Math.max(fairA, fairB);
    }
    /*
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * MR COMPUTE FAIR OBJECTIVE
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * Implementation:
     *
     * ---- ROUND 1 ----
     * Map each (Point, demographic class) pair to (Point, [demographic class, d_i])
     * where d_i is the squared distance between the point and the i-th centroid
     * computed by the LLoyd's algorithm. This produces a larger RDD.
     * Then we group elements by key which is the point (Shuffle phase) and the
     * reduce phase consists in taking the minimum distance for each point
     *
     * ---- ROUND 2 ----
     * to finish...
     */
    public static void MRPrintStatistics(JavaPairRDD<Vector, String> rdd, Vector[] centroids) {

    }

}
