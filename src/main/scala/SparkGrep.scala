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

import org.apache.spark.rdd.RDD
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
    def runTFI(s:RDD[(String,Array[String])]): RDD[(String,Array[String],Int,Array[(String,Double)])] =
    {
      val temp = s.map(value => (value._1, value._2.length))
      val tfi = s.map(f => (f._1, f._2, f._2.length, f._2.map(word => (word, f._2.filter(gene => gene == word).length.toDouble / f._2.length.toDouble))))
      tfi.foreach(x => println(x._4.foreach(y => println(y._1, y._2))))
      tfi.saveAsTextFile("bin/tfi")
      tfi
    }
    def runIDF(s:RDD[(String,Array[String])], d:Int): RDD[(String,Array[String],Int)] =
    {
      val temp = s.map(value => (value._1, value._2, d))
      //val idf = temp.map(x => (x._2,x._2.map(y => x._2.filter(z => z == y).length.toDouble)))
      val idf = temp.map(x => (x._2,x._2.map(y => y)))
        .map(x => (x._2,x._2.map(y => x._2.contains(y)).length.toDouble))
      idf.foreach(x => println(x._1(0),x._2))
      temp
    }
    println("Starting")
    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val documents = scala.io.Source.fromFile(args(1)).getLines.size
    val inputFile = sc.textFile(args(1)).cache()
    val counts = inputFile.map(document => (document.split("\t")(0), document.split("\t")(1).split(" ").filter(it => it.contains("gene_"))))
    //val tfi = runTFI(counts)
    val idf = runIDF(counts, documents)


    println("Spark code done")

    //counts.saveAsTextFile("bin/countsoutput")
    //pairs.saveAsTextFile("bin/pairsoutput")

    System.exit(0)
  }
}