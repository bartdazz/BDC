# Homeworks of Big Data

## Exercise 1
# How to run the code 
**To run the program on Intellij use as input "datasets/uber_small.csv 1 1 1". Or tu run the small test file (written by me) run "test_vectors.txt 1 1 1" (the "1 1 1" is used to assign the values of L, K, M).**

### What to do?
First we read the input file into an RDD. After that we have to remember that the last element of each row represents the class the point belongs to and that all the previous elements are the coordinates of the point and that have to be stored as a vector. To transform values in a vector we can use Vectors.dense().

The dataset has to be stored into another RDD of key value pairs as (vector of the point, demographic group) and then we can use this RDD to actually solve the exercise. 

We have to do the following:

- compute the set of centroids C with the LLoyd's algorithm
- compute the value of the objective functions 
- print some statisstic about the clustering

### Building a key-value pairs RDD
We have to apply the method flatMapToPair to the input RDD. The function we'll use should do the following:
- tokenize each point
- create a vector that will containt the first n-1 tokens transformed to Integers
- create the string class that will contain the last token
- create an ArrayList (named pairs) of Tuple2 of Vector and String to which we add each key-value pair
- return pairs.iterator()

### Counting the number of elements belonging to each demographic group
We can do this with a very simple Map Reduce algorithm. From a key value pairs RDD we set as key the demographic group and then we shuffle by key. In this way for the reduce phase we have as input a key which is the group and a value a list of points. We just have to print the length of the two lists.

### How to compute MRComputeStandardObjective?
The main thing is to be able to implement an efficient MR algorithm to compute the distance of each point from the set of centroids.
Possible idea: keep the point as the key of the pairs and as element put a list containing the distances of the point from all the centroids. Compute the minimum of this list and replace all the keys with the same value e.g. 0 (this could be done also in two steps to avoid having all the data in one reducer) and then sum the squared distances.
- Map Round 1: 

# Exercise 2:
- understand why the objective function are so slow (maybe we need to do an action on the RDD before the computation)
- sobstitute for cycle with map reduce algorithm
- check how the proportion between width and height changes the objective functions
- try to produce a file with a large number of centroids

