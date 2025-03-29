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
        sc.setLogLevel("WARN"); // same setting as the example

        // read the text file in input
        JavaRDD<String> points = sc.textFile(args[0]).cache();
        // to the previous line we can eventually add
        // .repartition(K)
        // to tell spark to divide the input in K (number to be choosen) partitions
        // inside the RDD

        int L = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int M = Integer.parseInt(args[3]);

        // print command-line arguments
        System.out.println("The number of partitions is: " + L);
        System.out.println("The number of centroids is: " + K);
        System.out.println("The number of iterations for the LLoyd's algorithm is: " + M);

        // SETTING GLOBAL VARIABLES
        long numpoints = points.count();
        System.out.println("The number of points is " + numpoints);

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
        long lenPoints = points.count();
        System.out.println("The number of points is " + lenPoints);

        // prova vettori
        // Create a vector using Vectors.dense
        // Vector v1 = Vectors.dense(1.0, 2.0, 3.0);
        // Vector v2 = Vectors.dense(4.0, 5.0, 6.0);
        // // Compute squared Euclidean distance
        // double distance = Vectors.sqdist(v1, v2);
        // // Print results
        // System.out.println("Vector 1: " + v1);
        // // Stop SparkContext at the end
        sc.close();
    }
}

class mymethods {
    // da mettere le 3 funzioni da implementare

    // MRComputeStandardObjective
    // MRComputeFairObjective
    // MRPrintStatistics

}
