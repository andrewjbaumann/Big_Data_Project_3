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
 * Useful Websites:
 * https://spark.apache.org/docs/latest/programming-guide.html
 * https://spark.apache.org/docs/latest/configuration.html
 * https://spark.apache.org/docs/latest/programming-guide.html
 */

import org.apache.spark.{SparkConf, SparkContext}

object SparkGrep {
  //dist is supposed to take 2 genes a return the edit distance
  //but i haven't decided how. whether to pass the rdd through
  //the function itself, or maybe just two strings, or even
  //possibly an array or something like that
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkGrep <host> <input_file>")
      System.exit(1)
    }
    println("Starting")

    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(1)).cache()
    val counts = inputFile.map(document => (document.split("\t")(0), document.split("\t")(1).split(" ").filter(it => it.contains("gene_"))))
    val pairs = counts.map(value => (value._1,value._2.length))
    val tfi = counts.map(value => (value._1,value._2.foreach(gene => ))

    tfi.foreach(value => println(value._1, value._2))
    pairs.foreach(value => println(value._1, value._2))


    println("Spark code done")
    pairs.saveAsTextFile("bin/output")

    System.exit(0)
  }
}