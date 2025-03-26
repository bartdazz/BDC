import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

// might run the file with java WordCountExample n_partitions input_file

public class WordCountExample {

    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }
        // You create an RDD from the file_path and then you create the partitions

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // we create the sparkconfig object
        // setAppName set your name or group bcs in cloud veneto it's useful for the
        // debug
        SparkConf conf = new SparkConf(true).setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // the following is to reduce the amount of information represent in output
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions

        // we create an RDD where each element is a string
        // each line of the text file is an element of the rdd
        // we can force the number of partitions with repartition
        // cache forces spark to keep in memory rdd (see lecture notes)
        JavaRDD<String> docs = sc.textFile(args[1]).repartition(K).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // heere we have the first action! It forces spark to create the rdd docs
        long numdocs, numwords;
        numdocs = docs.count();
        System.out.println("Number of documents = " + numdocs);
        // we define the object wordCounts which is the RDD of the final output
        // it consists of the word adn their counts
        // we use the class to have a RDD of key-valye pairs
        JavaPairRDD<String, Long> wordCounts;
        Random randomGenerator = new Random();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 1-ROUND WORD COUNT
        // you might fill one reducer with all the data if you have only one word
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // in the map phgase we have to go thourgh the doc, search for words and count
        // the partial
        // you apply to each element of the RDD a function
        //
        wordCounts = docs
                // as input you just receieve the documetn without the key
                .flatMapToPair((document) -> { // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    // hasmap to keep the counts
                    HashMap<String, Long> counts = new HashMap<>();
                    // define the array list that will contains the values
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    // add to the hashmap each token of the docuemnt
                    for (String token : tokens) {
                        // here you increase the dictionary
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    // ath this point we have a dictionary with all words and their count but we
                    // want a list of pairs
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        // for each element of the dictionary we have a key and the count?
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator(); // we produce an iterator of the set of pairs
                })

                // the folowing takes two elements with the same key and merges them
                // this means that produces an obj with the same key and a value that depeneds
                // on the first two
                // // in this case ww sum them
                // Look at the lecture notes
                .reduceByKey((x, y) -> x + y); // <-- REDUCE PHASE (R1)
        // the time of the execution up to now is very very fast bcs we just create an
        // rdd, ww don't have actions on the rdd
        // now we have an action
        numwords = wordCounts.count();
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 2-ROUND WORD COUNT - RANDOM KEYS ASSIGNED IN MAP PHASE
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        wordCounts = docs
        // with flatMapToPair you build a key-values RDD from a simple RDD
                .flatMapToPair((document) -> { // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, Long>>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    // up to here the map phase is the same as in the previous example
                    // remember that k is the number of partition we like to have
                    // randomGenerator.nextInt(K) is the new key !!
                    // and the actual pair (word-count) is the value

            //Map.Entry is used to iterate into entrySet
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(randomGenerator.nextInt(K), new Tuple2<>(e.getKey(), e.getValue())));
                    }
                    return pairs.iterator();
                })
                // shuffle: we group the pairs by their random key
                .groupByKey() // <-- SHFFLE+GROUPING
                // up to here we've created an RDD that has K elements?
                // now we apply a funciton to each one of this elements
                // we're inside a partition and we have to look how many elements we have and
                // define the partial counts
                // you see that the input is element: an element of the RDD
                .flatMapToPair((element) -> { // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> counts = new HashMap<>();
                    // _1 and _2 are the way we have to get the key and the value
                    // an element of the RDD is of the form
                    // (integer key < K, list of pairs)
                    // with ._1 you get the key and with ._2 you get the list
                    // with the next for loop you loop for each element of the list
                    for (Tuple2<String, Long> c : element._2()) {
                        // populate the dictionary with words as a key
                        // and increment the value wioth a sum
                        counts.put(c._1(), c._2() + counts.getOrDefault(c._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // up to now the RDD is of the form (word, count) but the same word may appear
                // many times
                // so we do the second round and we reduce by key to get the final result!!
                .reduceByKey((x, y) -> x + y); // <-- REDUCE PHASE (R2)
        numwords = wordCounts.count();
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 2-ROUND WORD COUNT - RANDOM KEYS ASSIGNED ON THE FLY
        // to show another funciton we change a little the way we produce random keys
        // we need the keys just one : for the shuffling
        // so we don't assing keys inside the RDD but just in the groupby
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        wordCounts = docs
                .flatMapToPair((document) -> { // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // the function groupby groups elements and we use a random key
                // for each element we create a random number just for the shhuffling
                .groupBy((wordcountpair) -> randomGenerator.nextInt(K)) // <-- KEY ASSIGNMENT+SHFFLE+GROUPING
                // now we have a list of values but we don't actually have the random key we
                // used to create the shuffle
                .flatMapToPair((element) -> { // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> counts = new HashMap<>();
                    for (Tuple2<String, Long> c : element._2()) {
                        counts.put(c._1(), c._2() + counts.getOrDefault(c._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .reduceByKey((x, y) -> x + y); // <-- REDUCE PHASE (R2)
        // reduceByKey is a narrow transformation: keeps the data in the same partition
        numwords = wordCounts.count();
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 2-ROUND WORD COUNT - SPARK PARTITIONS
        // an RDD is already splitted in partitions and so we can exploit them
        // See lecture notes
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        wordCounts = docs
                .flatMapToPair((document) -> { // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .mapPartitionsToPair((element) -> { // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> counts = new HashMap<>();
                    while (element.hasNext()) {
                        Tuple2<String, Long> tuple = element.next();
                        counts.put(tuple._1(), tuple._2() + counts.getOrDefault(tuple._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // at this point each partition has just one element with the same key
                // we could also use reduceByKey((x, y) -> x+y)
                .groupByKey() // <-- SHUFFLE+GROUPING
                .mapValues((it) -> { // <-- REDUCE PHASE (R2)
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                }); // Obs: one could use reduceByKey in place of groupByKey and mapValues
        numwords = wordCounts.count(); // count = # of elements in the RDD
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // COMPUTE AVERAGE WORD LENGTH
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int avgwordlength = wordCounts
                .map((tuple) -> tuple._1().length())
                .reduce((x, y) -> x + y);
        System.out.println("Average word length = " + avgwordlength / numwords);

    }

}
