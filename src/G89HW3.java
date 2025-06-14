import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.*;
import java.util.concurrent.Semaphore;

public class G89HW3 {

    public static void main(String[] args) throws Exception {
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: port, threshold," +
                    "rows of each sketch, columns of each sketch, number of top frequent items of interest");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("G89HW3");

        // Use batches of less than a second, otherwise you might exhaust the JVM
        // memory.
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

        int[][] CM = new int[D][W]; // matrices to compute count-min sketch
        int[][] CS = new int[D][W]; // matrices to compute count sketch
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0] = 0L;

        // store total occurrences of the elements of the stream
        // key = number, value = occurrences of the key
        Map<Long, Integer> dict_occurrences = new HashMap<>();

        // list with value and total occurrences to compute \phi(K)
        List<Tuple2<Long, Integer>> total_occ = new ArrayList<>();

        // list of Top-K heavy hitters
        List<Long> topk_hitters = new ArrayList<>();

        // intialize the hash functions
        hfun h1 = new hfun();
        hfun h2 = new hfun();
        hfun g = new hfun();
        h1.GenerateH(D, W);
        h2.GenerateH(D, W);
        g.GenerateH(D, W);
        // example of usage of the i-th function between the h1 ones: h1.myHash(x,i)
        // example of usage of the i-th function between the g ones: g.myHashG(x,i)

        // initialize the matrices CM and CS
        for (int j = 0; j < D; j++) {
            for (int i = 0; i < W; i++) {
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
                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            // COMPUTE TRUE FREQUENCIES
                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            /*
                             * get true frequency in the batch with map reduce:
                             * map : x -> (x,1), reduceBy key and sum the values
                             */
                            List<Tuple2<Long, Integer>> occurrence;
                            occurrence = batch.mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1))
                                    .reduceByKey((x, y) -> x + y).collect();

                            // merge the occurrences of the elements in this RDD in the dictionary of total
                            // occurrences
                            for (Tuple2<Long, Integer> pair : occurrence) {
                                dict_occurrences.put(pair._1(),
                                        dict_occurrences.getOrDefault(pair._1(), 0) + pair._2());
                            }

                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            // COMPUTE Count Sketch
                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            /*
                             * idea to compute the CS:
                             * We associate to each element s the key (j, h_j(s))
                             * and the value g_j(s),for each j = 0,...,d-1.
                             * h_j and g_j are the hash functions of the row j.
                             * Then we reduce by key and we sum the values
                             *
                             * After the Map Reduce we can easily build the d by w table using the
                             * coordinates and the values we've produced
                             */
                            List<Tuple2<Tuple2<Integer, Integer>, Integer>> res; // output MapReduce
                            res = batch.flatMapToPair(s -> {
                                Long x = Long.parseLong(s);
                                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>();
                                // compute all the hash functions
                                for (int j = 0; j < D; j++) {
                                    // coordinate = (row, hash value for the column)
                                    int[] coordinate = new int[2];
                                    // hash value
                                    int val = g.myHashG(x, j);
                                    coordinate[0] = j;
                                    coordinate[1] = h2.myHash(x, j);
                                    out.add(new Tuple2<>(new Tuple2<>(coordinate[0], coordinate[1]), val));
                                }
                                return out.iterator();
                            }).reduceByKey((x, y) -> x + y).collect();

                            // update the matrix CS with at most D*W iterations using the values computed in
                            // the MR
                            for (Tuple2<Tuple2<Integer, Integer>, Integer> entry : res) {
                                CS[entry._1()._1()][entry._1()._2()] += entry._2();
                            }

                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            // COMPUTE Count-Min Sketch
                            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                            // Same as Count Sketch but instead of using the hash function g,
                            // in the reduce phase we associate the element s to the value 1
                            List<Tuple2<Tuple2<Integer, Integer>, Integer>> rescm; // outuput MapReduce
                            rescm = batch.flatMapToPair(s -> {
                                Long x = Long.parseLong(s);
                                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out1 = new ArrayList<>();
                                for (int j = 0; j < D; j++) {
                                    int[] coordinate = new int[2];
                                    coordinate[0] = j;
                                    coordinate[1] = h1.myHash(x, j);
                                    out1.add(new Tuple2<>(new Tuple2<>(coordinate[0], coordinate[1]), 1));
                                }
                                return out1.iterator();
                            }).reduceByKey((x, y) -> x + y).collect();
                            // update the matrix CM with at most D*W iterations
                            for (Tuple2<Tuple2<Integer, Integer>, Integer> entry : rescm) {
                                CM[entry._1()._1()][entry._1()._2()] += entry._2();
                            }

                            // check the threshold
                            if (streamLength[0] >= THRESHOLD) {
                                // Stop receiving and processing further batches
                                stoppingSemaphore.release();
                            }
                        }
                    }
                });

        sc.start();
        stoppingSemaphore.acquire();

        // The following command stops the execution of the stream
        sc.stop(false, true);


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // COMPUTE AND PRINT FINAL STATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // print command-line arguments
        System.out.println("Port = " + portExp
                + " T = " + THRESHOLD
                + " D = " + D
                + " W = " + W
                + " K = " + K);

        /*
         * To compute the real occurrences of the values we extract from the dictionary
         * the keys and values, and we store ( key, value) in the List total_occ for each key,
         * then we sort by the number of occurrences in a non-increasing order
         * in respect of the value
         */
        total_occ = new ArrayList<>(dict_occurrences.size());
        for (Map.Entry<Long, Integer> entry : dict_occurrences.entrySet()) {
            Long key = entry.getKey();
            Integer value = entry.getValue();
            total_occ.add(new Tuple2<>(key, value));
        }
        total_occ.sort((a, b) -> b._2().compareTo(a._2()));
        // instead of having as in the example
        // 11, 10, 10, 9, 9, 9, 8, 8, 6, 6
        // in total_occ we have
        // ((number_1 with 11 occurrences,11),(number_2 with 10 occ,10),
        // (number_3 with 10 occ,10),...)
        topk_hitters = myMethodsHW3.topk(total_occ, K); // List<Long> of Top_K heavy hitters


        System.out.println("Number of processed items = " + streamLength[0]);
        System.out.println("Number of distinct items = " + dict_occurrences.size());
        System.out.println("Number of Top-K Heavy Hitters = " + topk_hitters.size());
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CM = "
                + myMethodsHW3.rel_err_CM(topk_hitters, dict_occurrences, CM, h1));
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CS = "
                + myMethodsHW3.rel_err_CS(topk_hitters, dict_occurrences, CS, h2, g));

        if (K <= 10) {
            System.out.println("Top-K Heavy Hitters:");
            for (Long e : topk_hitters) {
                System.out.println("Item " + e
                        + " True Frequency = " + dict_occurrences.get(e)
                        + " Estimated Frequency with CM = " + myMethodsHW3.CS_occ(CS, e, h2, g));
            }
        }



    }
}

class myMethodsHW3 {

    // Compute the relative error of the Top-K hitters in the Count Min
    public static double rel_err_CM(List<Long> topk_hitters, Map<Long, Integer> dict_occurrences, int[][] CM, hfun h) {
        // dict_occurrences = dictionary with real occurrences
        // topk_hitters = list of Top-K hitters
        // CM = count-min matrix
        // h = object of the class hfun containing the hash functions for Count-Min Sketch
        List<Double> results = new ArrayList<>(topk_hitters.size());
        double average = 0;
        for (Long val : topk_hitters) {
            double temp_occ = (double) CM_occ(CM, val, h);
            double temp_real_occ = (double) dict_occurrences.get(val);
            double temp_res = Math.abs(temp_occ - temp_real_occ) / temp_real_occ;
            results.add(temp_res);
        }
        for (double rel_err : results) {
            average += rel_err;
        }
        average = average / topk_hitters.size();
        return average;
    }

    // Compute the relative error of the top-K hitters in the Count Sketch
    public static double rel_err_CS(List<Long> topk_hitters, Map<Long, Integer> dict_occurrences, int[][] CS, hfun h,
            hfun g) {
        // dict_occurrences = dictionary with real occurrences
        // topk_hitters = list of Top-K hitters
        // CS = count-sketch matrix
        // h,g = object of the class hfun containing the hash functions for Count-Sketch
        List<Double> results = new ArrayList<>(topk_hitters.size());
        double average = 0;
        for (Long val : topk_hitters) {
            double temp_occ = (double) CS_occ(CS, val, h, g);
            double temp_real_occ = (double) dict_occurrences.get(val);
            double temp_res = Math.abs(temp_occ - temp_real_occ) / temp_real_occ;
            results.add(temp_res);
        }
        for (double rel_err : results) {
            average += rel_err;
        }
        average = average / topk_hitters.size();
        return average;
    }

    // Compute the occurrences givene the matrix of the Count Sketch
    public static int CS_occ(int[][] CS, Long u, hfun h, hfun g) {
        // CS = count-sketch matrix
        // h,g = object of the class hfun containing the hash functions for Count-Sketch
        // u = element
        ArrayList<Integer> f_us = new ArrayList<>(h.getD());
        for (int j = 0; j < h.getD(); j++) {
            f_us.add(g.myHashG(u, j) * CS[j][h.myHash(u, j)]);
        }
        return getMedian(f_us);
    }

    // Compute the occurrences given the matrix of the Count Min
    public static int CM_occ(int[][] CM, Long u, hfun h) {
        // Cm = count-min matrix
        // h= object of the class hfun containing the hash function for Count-Min
        // u = element

        // initialize the minimal value
        int min_val = CM[0][h.myHash(u, 0)];
        for (int j = 0; j < h.getD(); j++) {
            int temp_val = CM[j][h.myHash(u, j)];
            if (temp_val < min_val) {
                // update the min_val
                min_val = temp_val;
            }
        }
        return min_val;
    }

    // given a list of integer compute the median
    public static int getMedian(List<Integer> f_us) {
        Collections.sort(f_us);
        int n = f_us.size();
        if (n % 2 == 1) {
            return f_us.get(n / 2);
        } else {
            return f_us.get(n / 2 - 1);
        }
    }

    // Compute the top-K hitters
    public static List<Long> topk(List<Tuple2<Long, Integer>> total_occ, int K) {
        int phi_K = total_occ.get(K - 1)._2();
        List<Long> topk_hitters = new ArrayList<>();
        /*
         * top-K heavy hitters are defined as the items of u∈Σ whose true frequency is
         * fu≥ϕ(K)
         * so we have to iterate over the vector until i<K or the next
         */
        for (Tuple2<Long, Integer> t : total_occ) {
            if (t._2() >= phi_K) {
                topk_hitters.add(t._1());
            } else {
                break;
            }
        }
        return topk_hitters;
    }
}

// hash functions
class hfun implements java.io.Serializable {
    private int[][] V;
    private int[][] V2;
    private int D;
    private int W;

    public int getD() {
        return D;
    }


    // generate the values used for the hash functions
    public void GenerateH(Integer size, Integer mod) {
        // size = number of row
        // mod = number of column in CS and CM
        this.W = mod;
        this.V = new int[size][2];
        this.V2 = new int[size][2];
        this.D = size;
        Random rand = new Random();
        for (int j = 0; j < D; j++) {
            V[j][1] = rand.nextInt(8191);       // \in {0,1,...,8190}, values of b
            V[j][0] = rand.nextInt(8190) + 1;   // \in {1,2...,8190} values of a
            V2[j][1] = rand.nextInt(8191);      // \in {0,1,...,8190}, values of b
            V2[j][0] = rand.nextInt(8190) + 1;  // \in {1,2...,8190} values of a
        }
    }

    // to compute h_i(x) -> {0,...,W-1} for i = 1,...,D
    public int myHash(Long x, int i) {
        Long res1 = Math.floorMod((x * V[i][0]) + V[i][1], 8191);
        Long a = Math.floorMod(res1, W);
        int b = a.intValue();
        return b; // ((x*a +b) mod p) mod D
    }

    // to generate g_i(x) -> {+1,-1} for i = 1,...,D.
    // The idea is to generate a unique value given i and x such
    // that we can generate g_i unique for each i.
    // The unique value given i and x generated is used
    // as a key to generate a new instance of the Random class.
    // This ensures that g is a function which always returns the same
    // result for each given x and i, and the probability that g_i(x) = 1
    // for random x and i is exactly one half.
    public int myHashG(Long x, int i) {
        Random r = new Random();
        // We set the seed by using the values of V2
        // I used V2 instead of V otherwise if two element does a collision in myHash,
        // the same happen to myHashG
        r.setSeed(Math.abs(x * V2[i][0] + V2[i][1]));
        return (r.nextInt(2) == 0) ? 1 : -1;
    }

}
