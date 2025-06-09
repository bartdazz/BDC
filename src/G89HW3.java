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

        int[][] CM = new int[D][W]; // matrices to compute conservative count-min sketch
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&& THIS IS NOT CONSERVATIVE ANYMORE, RIGHT?? &&&&&&&&&&&&
        int[][] CS = new int[D][W]; // matrices to compute count sketch
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0] = 0L;
        // store total occurrences, key = number, value = occurrences of the key
        Map<Long, Integer> dict_occurrences = new HashMap<>();
        ;
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
        // exemple of usage of the i-th function between the h1 ones: h1.myHash(x,i)
        // exemple of usage of the i-th function between the g ones: g.myHashG(x,i)

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
                            // &&&&&&&&&&&&&&&&&&&&&&&&
                            // COMPUTE TRUE FREQUENCIES
                            // &&&&&&&&&&&&&&&&&&&&&&&&
                            /*
                             * get true frequency with map reduce:
                             * map : x -> (x,1), reduceBy key and sum the values
                             */
                            List<Tuple2<Long, Integer>> occurrance;
                            occurrance = batch.mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1))
                                    .reduceByKey((x, y) -> x + y).collect();

                            // merge the occurrences in the dictionary of total occurrences
                            for (Tuple2<Long, Integer> pair : occurrance) {
                                dict_occurrences.put(pair._1(),
                                        dict_occurrences.getOrDefault(pair._1(), 0) + pair._2());
                            }

                            // &&&&&&&&&&&&&
                            // COMPUTE CS
                            // &&&&&&&&&&&&&
                            /*
                             * idea to compute the CS:
                             * potremmo associare ad ogni elemento a ((row,col), val)
                             * così da non portarci dietro ogni cosa, poi raggrupare per key
                             * e sommare i secondo termini.
                             * una volta fatto con tutti gli elemnti costruiamo la matrice DxW
                             * con un unica iterazione sulla lista ((row_i,col_i), val)_{i = 1,...., D*W}
                             */
                            List<Tuple2<Tuple2<Integer, Integer>, Integer>> res; // outuput mapreduce
                            res = batch.flatMapToPair(s -> {
                                Long x = Long.parseLong(s);
                                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>();
                                for (int j = 0; j < D; j++) {
                                    int[] coordinate = new int[2];
                                    int val = g.myHashG(x, j);
                                    coordinate[0] = j;
                                    coordinate[1] = h2.myHash(x, j);
                                    out.add(new Tuple2<>(new Tuple2<>(coordinate[0], coordinate[1]), val));
                                }
                                return out.iterator();
                            }).reduceByKey((x, y) -> x + y).collect();
                            // update the matrix CS with at most D*W iterations
                            for (Tuple2<Tuple2<Integer, Integer>, Integer> entry : res) {
                                CS[entry._1()._1()][entry._1()._2()] += entry._2();
                            }
                            // &&&&&&&&&&&&&
                            // COMPUTE CM
                            // &&&&&&&&&&&&&
                            List<Tuple2<Tuple2<Integer, Integer>, Integer>> rescm; // outuput mapreduce
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

        // MANAGING STREAMING SPARK CONTEXT
        // System.out.println("Starting streaming engine");
        sc.start();
        // System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        // System.out.println("Stopping the streaming engine");

        /*
         * The following command stops the execution of the stream. The first boolean,
         * if true, also
         * stops the SparkContext, while the second boolean, if true, stops gracefully
         * by waiting for
         * the processing of all received data to be completed. You might get some error
         * messages when
         * the program ends, but they will not affect the correctness. You may also try
         * to set the second
         * parameter to true.
         */

        sc.stop(true, true);
        // System.out.println("Streaming engine stopped");

        // print command-line arguments
        System.out.println("Port = " + portExp
                + " T = " + THRESHOLD
                + " D = " + D
                + " W = " + W
                + " K = " + K);

        // COMPUTE AND PRINT FINAL STATISTICS
        /*
         * To compute the real occurrences of the values we extract from the dictionary
         * the key and values and store it the List total_occ, then we sort by the
         * number of
         * occurrences in a non-decreasing order in respect of the value
         *
         */
        total_occ = new ArrayList<>(dict_occurrences.size());
        for (Map.Entry<Long, Integer> entry : dict_occurrences.entrySet()) {
            Long key = entry.getKey();
            Integer value = entry.getValue();
            total_occ.add(new Tuple2<>(key, value));
        }
        total_occ.sort((a, b) -> b._2().compareTo(a._2()));
        // instead of having 11, 10, 10, 9, 9, 9, 8, 8, 6, 6
        // in total_occ we have ((number_1 with 11 occ,11),(number_2 with 10
        // occ,10),(number_3 with 10 occ,10),...)
        topk_hitters = myMethodsHW3.topk(total_occ, K); // List<Long> of top_K heavy hitters

        ////////////////////
        // DEBUG
        ////////////////////
        /*
         * System.out.println("top K heavy hitters: " + topk_hitters); // prova
         * System.out.println("Items with the most occ:" + total_occ.get(0)); // prova
         * System.out.println("calcolato da CS: " + total_occ.get(0)._1()+","+
         * myMethodsHW3.CS_occ(CS,total_occ.get(0)._1(), h2,g));
         * System.out.println("calcolato da CM: " + total_occ.get(0)._1()+","+
         * myMethodsHW3.CM_occ(CM,total_occ.get(0)._1(), h1));
         */

        System.out.println("Number of processed items = " + streamLength[0]);
        System.out.println("Number of distinct items = " + dict_occurrences.size()); // da tenere
        System.out.println("Number of Top-K Heavy Hitters = " + topk_hitters.size());
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CM = "
                + myMethodsHW3.rel_err_CM(topk_hitters, dict_occurrences, CM, h1));
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CS = "
                + myMethodsHW3.rel_err_CS(topk_hitters, dict_occurrences, CS, h2, g)); // da tenere

        if (K <= 10) {
            System.out.println("Top-K Heavy Hitters:");
            List<Tuple2<Long, Integer>> topk_true_frq = new ArrayList<>();
            List<Tuple2<Long, Integer>> topk_est_frq = new ArrayList<>();
            for (Long e : topk_hitters) {
                System.out.println("Item " + e
                        + " True Frequency = " + dict_occurrences.get(e)
                        + " Estimated Frequency with CM = " + myMethodsHW3.CS_occ(CS, e, h2, g));
            }
            for (Long e : topk_hitters) {
                topk_true_frq.add(new Tuple2<>(e, dict_occurrences.get(e)));
                topk_est_frq.add(new Tuple2<>(e, myMethodsHW3.CS_occ(CS, e, h2, g)));
            }
            System.out.println("true top-" + K + " heavy hitters frequencies: " + topk_true_frq);
            System.out.println("estimated top-" + K + " heavy hitters frequencies: " + topk_est_frq);

        }

    }
}

class myMethodsHW3 {
    /*
     * TOLTO PERCHè NON BISOGNA PIù USARE CONSERVATIVE COUNT MIN
     * public static void updateCM(int[][] C, Long u, hfun h){
     * int D = h.getD();
     * List<Tuple2<Integer, Integer>> pairs = new ArrayList<>(); // pairs to be
     * updated
     * int val_min = C[0][h.myHash(u,0)]; // initialize the minimum value
     * 
     * for(int i = 0; i<D; i++){
     * int[] temp_pairs = {i,h.myHash(u,i)};
     * int temp_val = C[temp_pairs[0]][temp_pairs[1]];
     * if(temp_val == val_min){
     * // if the current value of i-th row is the same to the
     * // minimal value add the coordinate to the pairs
     * pairs.add(new Tuple2<>(i,h.myHash(u,i)));
     * }
     * if(temp_val < val_min){
     * // if the current value of i-th row is the same to the
     * // minimal value we are not interested in the pairs we selected,
     * // we delete all the ones saved and add the new one.
     * // we also update the val_min
     * val_min = temp_val;
     * pairs.clear(); // Removes all elements from the list
     * pairs.add(new Tuple2<>(temp_pairs[0],temp_pairs[1]));
     * }
     * // if temp_val > val_min we don't care about the current temp_pairs
     * }
     * // now we can update the C matrix
     * for(Tuple2<Integer, Integer> p : pairs){
     * C[p._1()][p._2()] += 1;
     * }
     * }
     */
    public static double rel_err_CM(List<Long> topk_hitters, Map<Long, Integer> dict_occurrences, int[][] CM, hfun h) {
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

    public static double rel_err_CS(List<Long> topk_hitters, Map<Long, Integer> dict_occurrences, int[][] CS, hfun h,
            hfun g) {
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

    public static int CS_occ(int[][] CS, Long u, hfun h, hfun g) {
        ArrayList<Integer> f_us = new ArrayList<>(h.getD());
        for (int j = 0; j < h.getD(); j++) {
            f_us.add(g.myHashG(u, j) * CS[j][h.myHash(u, j)]);
        }
        return getMedian(f_us);
    }

    public static int CM_occ(int[][] CM, Long u, hfun h) {
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

    public static int getMedian(List<Integer> f_us) {
        Collections.sort(f_us);
        int n = f_us.size();
        if (n % 2 == 1) {
            return f_us.get(n / 2);
        } else {
            return f_us.get(n / 2 - 1);
        }
    }

    /*
     * top-K heavy hitters are defined as the items of u∈Σ whose true frequency is
     * fu≥ϕ(K)
     * so we have to iterate over the vector until i<K or the next
     */
    public static List<Long> topk(List<Tuple2<Long, Integer>> total_occ, int K) {
        int phi_K = total_occ.get(K - 1)._2();
        List<Long> topk_hitters = new ArrayList<>();
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

class hfun implements java.io.Serializable {
    private int[][] V;
    private int[][] V2;
    private int D;
    private int W;

    public int getD() {
        return D;
    }

    public int getW() {
        return W;
    }

    public void GenerateH(Integer size, Integer mod) {
        this.W = mod;
        this.V = new int[size][2];
        this.V2 = new int[size][2];
        this.D = size;
        Random rand = new Random();
        for (int j = 0; j < D; j++) {
            V[j][1] = rand.nextInt(8191); // \in {0,1,...,8190} valori di b
            V[j][0] = rand.nextInt(8190) + 1; // \in {1,2...,8190} valori di a
            V2[j][1] = rand.nextInt(8191); // \in {0,1,...,8190} valori di b
            V2[j][0] = rand.nextInt(8190) + 1; // \in {1,2...,8190} valori di a
        }
    }

    public int myHash(Long x, int i) {
        Long res1 = Math.floorMod((x * V[i][0]) + V[i][1], 8191);
        Long a = Math.floorMod(res1, W);
        int b = a.intValue();
        return b; // ((x*a +b) mod p) mod D
    }

    // to generate g_i(x) -> {+1,-1} for i = 1,...,D and x an .
    // The idea is to generate a unique value given i and x such
    // that we can generate g_i unique
    public int myHashG(Long x, int i) {
        Random r = new Random();
        // We set the seed by using the values of V
        // I used V2 instead of V because if two element does a collision in myHash,
        // the same happen to myHashG
        r.setSeed(Math.abs(x * V2[i][0] + V2[i][1]));
        return (r.nextInt(2) == 0) ? 1 : -1;
    }

}
