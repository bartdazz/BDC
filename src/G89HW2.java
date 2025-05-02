import java.io.IOException;

import org.apache.commons.math3.analysis.function.Sqrt;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

import java.util.*;

public class G89HW2 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: file_path, num_partition, num_centers, num_iteration");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Create Spark configuration
        SparkConf conf = new SparkConf().setAppName("G89HW1");
        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Reading the input
        int L = Integer.parseInt(args[1]); // number of partitions of the RDD
        int K = Integer.parseInt(args[2]); // number of clusters
        int M = Integer.parseInt(args[3]); // number of iterations

        // read the text file in input
        JavaRDD<String> points = sc.textFile(args[0]).cache().repartition(L);
        long numpoints = points.count();
        // build a key-value pairs RDD
        JavaPairRDD<Vector, String> inputPoints;

        // the following chunk of code will create a key-value pairs RDD
        inputPoints = points
                .flatMapToPair((pointClass) -> {
                    // splitting the String representing the point and it's class
                    String[] tokens = pointClass.split(",");

                    // getting the length of the array tokens
                    Integer len = tokens.length;

                    // Extracting the sublist regarding the point's coordinates
                    String[] pointString = Arrays.copyOfRange(tokens, 0, len - 1);

                    // Transforming the coordinates into doubles
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
            if (Objects.equals(classCount._1(), "A")) {
                classA = classCount._2();
            } else {
                classB = classCount._2();
            }
        }
        // print command-line arguments
        System.out.println("Input file = " + args[0]
                + ", L = " + L
                + ", K = " + K
                + ", M = " + M);

        System.out.println("N = " + numpoints
                + ", NA = " + classA
                + ", NB = " + classB);

        // compute clusters with the LLoyd's algorithm
        KMeansModel clusters = KMeans.train(inputPoints.map(Tuple2::_1).rdd(), K, M);
        Vector[] cStand = clusters.clusterCenters();

        Vector[] cFair = myMethods.MRFairLloyd(inputPoints, K, M);

        // Print the value of the objective functions
        System.out.println("Phi(A,B,C_stand) = " + mymethods.MRComputeFairObjective(inputPoints, cStand));
        System.out.println("Phi(A,B,C_fair) = " + mymethods.MRComputeFairObjective(inputPoints, cFair));

        // ####################################################################
        // PRINT TIME STATISTICS
        // ####################################################################
        // Stop SparkContext at the end
        sc.close();
    }
}

class myMethods {
    public static Vector[] MRFairLloyd(JavaPairRDD<Vector, String> U, Integer K, Integer M) {
        /*
         * Input:
         * U : key-value pairs RDD where the key is a vector representing a point and
         * the key is the corresponding demographic class
         * K : number of clusters
         * M : number of iterations of the algorithm
         *
         * Output:
         * Array C of vectors that are the centroids of the clustering
         */
        // %%%%%%%%%%%%%%%%%%%%%%
        // Code to compute NA, NB
        // %%%%%%%%%%%%%%%%%%%%%%
        JavaPairRDD<String, Integer> classItems;
        classItems = U
                .mapToPair((element) -> new Tuple2<>(element._2(), 1))
                .reduceByKey((x, y) -> x + y);

        // The function collect takes all the data stored in the RDD and puts
        // it into a list; the RDD is sufficiently small to allow us to do so
        List<Tuple2<String, Integer>> classCounts = classItems.collect();

        // Print the number of elements in each class
        // Each element in the for loop is a Tuple2
        // in classCount._1() there is the class name (key)
        // in classCount._2() there is the count of elements in that class (value)
        int classA = 0, classB = 0;
        for (Tuple2<String, Integer> classCount : classCounts) {
            if (Objects.equals(classCount._1(), "A")) {
                classA = classCount._2();
            } else {
                classB = classCount._2();
            }
        }
        // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        // End of code to compute NA, NB
        // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        // Initialization of the set C of cetroids using kmeans|| (0 iteration of
        // Lloyd's algorithm
        KMeansModel clusters = KMeans.train(U.map(Tuple2::_1).rdd(), K, 0);
        Vector[] C = clusters.clusterCenters();

        // loop of the algorithm

        for (int i = 0; i < M; i++) {
            // build the list that will collect the value of the map-reduced RDD
            List<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Vector>>> auxSums;
            auxSums = U.flatMapToPair((element) -> {
                // List that will store
                // ((cluster's center index, demographic class), (1, point))
                ArrayList<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Vector>>> clusterPoint = new ArrayList<>();

                double minDistance = Vectors.sqdist(element._1(), C[0]);
                int closerCenter = 0;
                for (int j = 0; j < C.length; j++) {
                    if (Vectors.sqdist(element._1(), C[j]) < minDistance) {
                        closerCenter = j;
                    }
                    clusterPoint.add(new Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Vector>>(
                            new Tuple2<Integer, String>(closerCenter, element._2()),
                            new Tuple2<Integer, Vector>(1, element._1())));
                }
                return clusterPoint.iterator();
            }).reduceByKey((x, y) -> {
                // this method has to return element of the same type of the input
                int numElements = x._1() + y._1();
                Vector sumVectors = myMethods.SumVectors(x._2(), y._2());
                return new Tuple2<Integer, Vector>(numElements, sumVectors);
            }).collect();

            // initialize variables as arrays of size K = number of clusters
            double[] alpha = new double[K];
            double[] beta = new double[K];
            Vector[] muA = new Vector[K];
            Vector[] muB = new Vector[K];
            double[] l = new double[K];

            for (Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Vector>> element : auxSums) {
                int clusterCenterIdx = element._1()._1();
                String demoClass = element._1()._2();
                int numElements = element._2()._1();
                Vector sumVectors = element._2()._2();

                // update variables
                if (demoClass.equals("A")) {
                    alpha[clusterCenterIdx] = numElements / classA;
                    muA[clusterCenterIdx] = myMethods.VectorDivision(sumVectors, numElements);
                } else {
                    beta[clusterCenterIdx] = numElements / classB;
                    muB[clusterCenterIdx] = myMethods.VectorDivision(sumVectors, numElements);
                }
            }
            for (int j = 0; j < l.length; j++) {
                l[j] = Math.sqrt(Vectors.sqdist(muA[i], muB[i]));
            }

            // end of for cycle of the algorithm
        }

        return C;
    }

    public static Vector SumVectors(Vector v1, Vector v2) {
        double[] sum = new double[v1.size()];
        for (int i = 0; i < v1.size(); i++) {
            sum[i] = v1.apply(i) + v2.apply(i);
        }
        return Vectors.dense(sum);
    }
    public static Vector VectorDivision(Vector v, Integer n) {
        double[] res = new double[v.size()];
        for (int i = 0; i < v.size(); i++) {
            res[i] = v.apply(i) / n;
        }
        return Vectors.dense(res);
    }

    /*
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * MR COMPUTE FAIR OBJECTIVE
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * We compute NA and NB by mapping each point to (class of the point, 1), then
     * we group by
     * key and sum the elements.
     * ---- Round 1 ----
     * Map each (Point, demographic class) pair to (class of the point, smaller
     * distance).
     * It computes the distance between the point and all the centroids computed by
     * the LLoyd's algorithm,
     * then it choose the smallest distance and map each point to (class of the
     * point, smaller distance).
     * Then group elements by key and sum all the distances.
     * The output is a rdd with for each two classes the sum of the distances.
     */
    public static double MRComputeFairObjective(JavaPairRDD<Vector, String> rdd, Vector[] centroids) {
        // %%%%%%%%%%%%%%%%%%%%%%
        // Code to compute NA, NB
        // %%%%%%%%%%%%%%%%%%%%%%
        JavaPairRDD<String, Integer> classItems;
        classItems = rdd
                .mapToPair((element) -> new Tuple2<>(element._2(), 1))
                .reduceByKey((x, y) -> x + y);

        // The function collect takes all the data stored in the RDD and puts
        // it into a list; the RDD is sufficiently small to allow us to do so
        List<Tuple2<String, Integer>> classCounts = classItems.collect();

        // Print the number of elements in each class
        // Each element in the for loop is a Tuple2
        // in classCount._1() there is the class name (key)
        // in classCount._2() there is the count of elements in that class (value)
        int classA = 0, classB = 0;
        for (Tuple2<String, Integer> classCount : classCounts) {
            if (Objects.equals(classCount._1(), "A")) {
                classA = classCount._2();
            } else {
                classB = classCount._2();
            }
        }
        // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        // End of code to compute NA, NB
        // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        // Round 1
        JavaPairRDD<String, Double> FairObjective; // output RDD

        FairObjective = rdd.flatMapToPair((pair) -> {
            ArrayList<Tuple2<Vector, Tuple2<String, Double>>> pointDistancesClass = new ArrayList<>();
            Tuple2<Vector, Tuple2<String, Double>> pointDistancesClassOut;
            Vector point = pair._1(); // point
            String demoClass = pair._2(); // demographic class
            for (Vector center : centroids) {
                // compute the distance between the selected center and the point
                double distance = Vectors.sqdist(point, center);
                // Tuple containing the demographic class and the distance
                Tuple2<String, Double> classDistance = new Tuple2<String, Double>(demoClass, distance);
                pointDistancesClass.add(new Tuple2<Vector, Tuple2<String, Double>>(point, classDistance));
            }
            // initialize the variable
            pointDistancesClassOut = new Tuple2<Vector, Tuple2<String, Double>>(
                    pointDistancesClass.get(0)._1(), pointDistancesClass.get(0)._2());
            // find the smallest distance
            for (Tuple2<Vector, Tuple2<String, Double>> el : pointDistancesClass) {
                if (el._2()._2() < pointDistancesClassOut._2()._2()) {
                    pointDistancesClassOut = new Tuple2<Vector, Tuple2<String, Double>>(el._1(), el._2());
                }
            }
            // in element._2()._1() there is the class, A or B
            // in element._2()._2() there is the distance
            ArrayList<Tuple2<String, Double>> output = new ArrayList<>();
            // (class of the point, smaller distance)
            output.add(new Tuple2<String, Double>(pointDistancesClassOut._2()._1(),
                    pointDistancesClassOut._2()._2()));
            return output.iterator();
        })
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
}
