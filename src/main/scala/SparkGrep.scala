/**
 * Title:       SparkGrep.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Created on:  4/24/2015
 * Modified on: 4/26/2015
 * Description: Problem
 *              – Given a number of documents, compute the word semantic similarity using MapReduce
 *              – Input: A text file, each line represents a document
 *              – Output: A list of term-term pairs sorted by their similarity descending
 *              - t1 t2 s1
 *              - t3 t4 s2
 *              • Sub-problems:
 *              – Compute Term Frequency – Inverse Document Frequency (TF-IDF) for each term
 *              – Computer term similarity
 *              – Sort the term similarity
 * Build with:  Scala IDE (Eclipse or IntelliJ) or using the following commands on the glab machines
 *              To compile: scalac *.scala
 *              To run:     scala SparkGrep <host> <input_file>
 */

import org.apache.spark.{SparkConf, SparkContext}

object SparkGrep {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkGrep <host> <input_file>")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(1), 2).cache()
    val counts = inputFile.flatMap(line => line.split(" "))
      .map(word => (word))
    //counts.saveAsTextFile("bin/output")
    val arrayCounts = counts.toArray()
    var finalArray:List[String] = List()
    iterate(arrayCounts)

    def iterate(theArray:Array[String]): Unit ={
      for (x <- theArray) {
        if (theArray.head.toString().startsWith("gene")) {
          finalArray = finalArray.::(arrayCounts.head.toString())
        }
      }
      iterate(theArray.tail)
    }


    System.exit(0)
  }
}
