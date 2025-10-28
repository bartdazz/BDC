# University Project: Spark Algorithms

This repository contains the Java source code for three exercises completed for a university course, focusing on the implementation of distributed algorithms using Apache Spark.

---

## Exercise 1 (G89HW1): K-Means Evaluation

This program first executes the standard K-Means (Lloyd's) algorithm on a given dataset, which is partitioned into two demographic groups (A and B).

After the $K$ cluster centers are computed, the program runs three distinct MapReduce algorithms to evaluate the quality and fairness of the clustering:

1.  **Standard Objective:** Computes the standard K-Means objective, which is the average squared distance of each point to its nearest cluster center.
2.  **Fairness Objective:** Computes a fairness metric, defined as the **maximum** of the average squared distances for the two demographic groups (A and B) considered separately.
3.  **Cluster Statistics:** Computes the demographic composition of each cluster, reporting the number of points from group A and group B assigned to each of the $K$ centers.

---

## Exercise 2 (G89HW2): Fair K-Means Implementation

This exercise implements a fair clustering algorithm, `MRFairLloyd`, using an iterative MapReduce approach. This algorithm modifies the standard K-Means update step to optimize for the fairness objective introduced in Exercise 1.

In each of the $M$ iterations, the algorithm performs the following:

1.  **Assignment:** Assigns all points to their nearest center (from the previous iteration).
2.  **Fair Center Update:** Computes a new set of centers. This update is not the simple cluster centroid. Instead, it calculates a new center position that is a carefully weighted combination derived from the means of the two demographic groups (A and B) within that cluster. This position is calculated to minimize the fairness objective.

The program's main output is a comparison of the fairness score achieved by these "fair centers" versus the score achieved by the "standard centers" from a regular K-Means execution.

---

## Exercise 3 (G89HW3): Streaming Frequency Sketches

This exercise uses Spark Streaming to implement and evaluate two probabilistic frequency-counting algorithms (sketches) from a live data stream: **Count-Min Sketch** and **Count-Sketch**.

The program processes a stream of items in batches until a specified threshold $T$ of total items is met. For each incoming batch, it performs three tasks in parallel using MapReduce:

1.  **Count-Min Update:** Computes and applies the updates to the $D \times W$ Count-Min sketch matrix.
2.  **Count-Sketch Update:** Computes and applies the updates to the $D \times W$ Count-Sketch matrix.
3.  **Ground Truth:** Calculates the *exact* frequencies of items in the batch and updates a global hash map to maintain the true count for all items seen so far.

Once the stream processing stops (after $T$ items), the program identifies the true Top-$K$ most frequent items. It then evaluates the accuracy of both sketches by computing the Average Relative Error (ARE) between their frequency estimates and the true frequencies for these $K$ items.