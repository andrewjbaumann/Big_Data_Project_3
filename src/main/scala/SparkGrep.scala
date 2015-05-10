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

import scala.math._


object SparkGrep {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkGrep <host> <input_file>")
      System.exit(1)
    }
    println("Starting")


    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(1), 2).cache()
    val counts = inputFile.flatMap(line => line.split(" "))
      .filter(word => word.contains("gene_"))
      .distinct()
      

    println("Spark code done")

    counts.saveAsTextFile("bin/output")

    def distance(s1: String, s2: String): Int = {
      def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
      val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

      for (j <- 1 to s2.length; i <- 1 to s1.length)
        dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
      dist(s2.length)(s1.length)
    }
    System.exit(0)
  }
}


//Previous Code
/*
def Looking_for_genes(s: String): Unit = {
  val tempCounts = counts.toArray()
  val tempList: Set[String] = tempCounts.toSet
  var aList: Set[String] = Set()

  println("Starting iterate...")

  iterate(tempList)
  def iterate(theCounters: Set[String]): Unit = {
    if (theCounters.size == 0)
      return
    if (theCounters.head.startsWith("gene_")) {
      aList = aList + theCounters.head
    }
    iterate(theCounters.tail)
  }

  val aRay: Array[String] = aList.toArray
  var secondRay: Array[String] = aList.toArray
  var finalArray = Array[Tuple2[String, String]]()
  consolidate(aRay)

  def consolidate(array: Array[String]): Unit = {
    for (x <- 0 until (array.length - 1)) {
      var temp = array(x + 1)

      for (y <- x + 1 until (array.length - 1))
        if (distance(array(x), temp) < distance(array(x), array(y)))
          temp = array(y)

      val t = (array(x), temp)
      finalArray = finalArray :+ t
    }
  }
  finalArray.foreach(x => println(x))
}*/
