import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class G89HW3 {

    public static void main(String[] args) throws Exception {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: port, threshold, number of items," +
                    "rows of each sketch, columns of each sketch, number of top frequent items of interest");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName("G89HW3");

        // Use batches of less than a second, otherwise you might exhaust the JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int portExp = Integer.parseInt(args[0]);
        int THRESHOLD = Integer.parseInt(args[1]);
        int D = Integer.parseInt(args[2]);
        int W = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int[][] CM = new int[D][W]; // matrices to compute  conservative count-min sketch
        int[][] CS = new int[D][W]; // matrices to compute  count sketch
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements

        // intialize the hash functions
        hfun h1 = new hfun();
        hfun h2 = new hfun();
        hfun g = new hfun();
        h1.GenerateH(D);
        h2.GenerateH(D);
        g.GenerateH(D);
        // exemple of usage of the i-th function between the h1 ones: h1.myHash(x,i)
        // exemple of usage of the i-th function between the g ones: g.myHashG(x,i)




        //initialize the matrices CM and CS
        for(int j = 0;j<D;j++) {
            for (int i = 0; i<W;i++){
                CM[j][i] = 0;
                CS[j][i] = 0;
            }
        }

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        if (batchSize > 0) {

                            // list of Row and Column to be increased in CM
                            ArrayList<Tuple2<Integer, Integer>> RowCol = new ArrayList<>();

                            //System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                            // Extract the distinct items from the batch
                            Map<Long, Long> batchItems = batch
                                    .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                    .reduceByKey((i1, i2) -> 1L)
                                    .collectAsMap();
                            // Update the streaming state. If the overall count of processed items reaches the
                            // THRESHOLD value (among all batches processed so far), subsequent items of the
                            // current batch are ignored, and no further batches will be processed
                            for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                                if (!histogram.containsKey(pair.getKey())) {
                                    histogram.put(pair.getKey(), 1L);
                                }
                            }
                            // If we wanted, here we could run some additional code on the global histogram
                            if (streamLength[0] >= THRESHOLD) {
                                // Stop receiving and processing further batches
                                stoppingSemaphore.release();
                            }

                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        /* The following command stops the execution of the stream. The first boolean, if true, also
           stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
           the processing of all received data to be completed. You might get some error messages when
           the program ends, but they will not affect the correctness. You may also try to set the second
           parameter to true.
        */

        sc.stop(false, true);
        System.out.println("Streaming engine stopped");

        // print command-line arguments
        System.out.println("Receiving data from port = " + portExp
                + ", Threshold = " + THRESHOLD
                + ", D = " + D
                + ", W = " + W
                + ", K = " + K);

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of distinct items = " + histogram.size()); // da tenere
        long max = 0L;
        ArrayList<Long> distinctKeys = new ArrayList<>(histogram.keySet());
        Collections.sort(distinctKeys, Collections.reverseOrder());
        System.out.println("Largest item = " + distinctKeys.get(0));


    }
}

class myMethodsHW3 {
    }

class hfun{
    private int[][] V;
    private int[][] V2;
    private int D;
    public void GenerateH(Integer size){
        this.V = new int[size][2];
        this.V2 = new int[size][2];
        this.D = size;
        Random rand = new Random();
        for(int j = 0;j<D;j++) {
            V[j][1] = rand.nextInt(8191); //  \in {0,1,...,8190} valori di b
            V[j][0] = rand.nextInt(8190) + 1; //  \in {1,2...,8190} valori di a
            V2[j][1] = rand.nextInt(8191); //  \in {0,1,...,8190} valori di b
            V2[j][0] = rand.nextInt(8190) + 1; //  \in {1,2...,8190} valori di a
        }
    }
    public int myHash(int x,int i){
        return (((x * V[i][0]) + V[i][1])%8191)%D; // ((x*a +b) mod p) mod D
    }


    // to generate g_i(x) -> {+1,-1} for i = 1,...,D and x an .
    // The idea is to generate a unique value given i and x such
    // that we can generate g_i unique
    public int myHashG(int x,int i){
        Random r = new Random();
        // We set the seed by using the values of V
        // I used V2 instead of V because if two element does a collision in myHash,
        // the same happen to myHashG
        r.setSeed(Math.abs(x*V2[i][0] + V2[i][1]));
        return (r.nextInt(100000000)%2 == 0) ? 1 : -1;
    }

}
