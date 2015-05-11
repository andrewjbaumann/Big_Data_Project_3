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

import scala.math._

object SparkGrep {
  //dist is supposed to take 2 genes a return the edit distance
  //but i haven't decided how. whether to pass the rdd through
  //the function itself, or maybe just two strings, or even
  //possibly an array or something like that

  def dist(a:Array[String], c:String): String = {
    def distance(s1: String, s2: String): Int = {
      def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
      val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

      for (j <- 1 to s2.length; i <- 1 to s1.length)
        dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
      dist(s2.length)(s1.length)
    }
    var min_string = ""
    for(x <- 0 to a.size-1)
    {
      var max_dist = 0

      var compute = distance(c, a(x))

      if(compute > max_dist)
      {
        max_dist = compute
        min_string = a(x)
      }
    }
    println(min_string)
    min_string
  }

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
    /*
    This code snippet is supposed to take the whole list of genes and compare them to every other gene in the list,
    but it's way too large and takes way too long. Spark doesn't let you call another RDD inside of an RDD
    (for example, letting a new var RDD map the words into tuples of (word, distance(counts, word)) won't work
    because you can't call an RDD inside of another. So I don't think at this point there's a really elegant way
    for spark to handle something of this kind... but at the same time, that sounds kind of dumb, but i'll figure it
    out tuesday.
    val array = counts.toArray()
    val results = counts
      .map(word => (word, dist(array, word)))
     */

    println("Spark code done")

    counts.saveAsTextFile("bin/output")
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
