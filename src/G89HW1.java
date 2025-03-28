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

        // read the input
        JavaRDD<String> points = sc.textFile(args[0]).cache();
        // to the previous line we can eventually add
        //.repartition(K) 
        // to tell spark to divide the input in K (number to be choosen) partitions
        // inside the RDD

        int L = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int M = Integer.parseInt(args[3]);
        
        // SETTING GLOBAL VARIABLES
        long numpoints = points.count();
        System.out.println("The number of points is " + numpoints);

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
    /*
    descrizione intuitiva
     */
    // MRComputeFairObjective
    // MRPrintStatistics

}
