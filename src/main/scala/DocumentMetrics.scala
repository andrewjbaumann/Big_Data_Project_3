/**
 * Title:           SparkGrep.scala
 * Authors:         Andrew Baumann, Tony Zheng
 * Created on:      4/24/2015
 * Modified on:     4/26/2015
 * Description:     Problem
 *                  – Given a number of documents, compute the word semantic similarity using MapReduce
 *                  – Input: A text file, each line represents a document
 *                  – Output: A list of term-term pairs sorted by their similarity descending
 *                  - t1 t2 s1
 *                  - t3 t4 s2
 *                  • Sub-problems:
 *                  – Compute Term Frequency – Inverse Document Frequency (TF-IDF) for each term
 *                  – Computer term similarity
 *                  – Sort the term similarity
 * Build with:      Scala IDE (Eclipse or IntelliJ) or using the following commands on the glab machines
 *                  To compile: scalac *.scala
 *                  To run:     scala SparkGrep <host> <input_file>
 * Useful Websites: https://spark.apache.org/docs/latest/programming-guide.html
 *                  https://spark.apache.org/docs/latest/configuration.html
 *                  https://spark.apache.org/docs/latest/programming-guide.html
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object DocumentMetrics {

  private var documentSize = 0.0

  def startConfig(host:String, input_file:String):Unit = {
    println("Starting")

    val conf = new SparkConf().setAppName("SparkGrep").setMaster(host)
    val sc = new SparkContext(conf)

    val documents = setDocumentSize(input_file)
    val inputFile = sc.textFile(input_file).cache()
    val counts = inputFile.map(document => (document.split("\t")(0), document.split("\t")(1).split(" ").filter(it => it.contains("gene_"))))
    val tf = runTF(counts)
    val idf = runIDF(counts, documentSize)
    //val tfidf = runCombineTFIDF(tf, idf)
    //runSemanticSimilarity(tfidf)

    println("Spark code done")

    //counts.saveAsTextFile("bin/countsoutput")
    //pairs.saveAsTextFile("bin/pairsoutput")
  }

  def setDocumentSize(x:String):Unit = {
    documentSize = scala.io.Source.fromFile(x).getLines.size.toDouble
  }

  def runTF(s:RDD[(String, Array[String])]):RDD[(String, Double)] = {
    val temp = s.map(value => (value._1, value._2.length))
    val tfi = s.map(f => f._2.map(word => (word, f._2.filter(gene => gene == word).length.toDouble / f._2.length.toDouble)))
      .flatMap(x => x)
      .map(x => (x._1, x._2))
      .sortByKey()
    tfi.saveAsTextFile("bin/tf")
    return tfi
  }

  def CountGenesIDF(g:String, s:(Array[String])):Int = {
    if (s.contains(g))
      return 1
    else
      return 0
  }

  def runIDF(s:RDD[(String, Array[String])], d:Double):RDD[(String, Double)] = {
    val temp = s.map(value => (value._1, value._2.distinct, d))
    val idf = s.map(f => f._2.map(gene => (gene, CountGenesIDF(gene, f._2))).distinct)
      .flatMap(x => x)
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, math.log(d / (x._2.toDouble))))
      .sortByKey()
    idf.saveAsTextFile("bin/idf")
    return idf
  }

  def runCombineTFIDF(t:RDD[(String, Double)], i:RDD[(String, Double)]):RDD[(String, Iterable[Double])] = {
    val tfidf = t.join(i)
      .map(x => (x._1, x._2._1 * x._2._2))
      .groupByKey()
    tfidf.saveAsTextFile("bin/tfidf")
    return tfidf
  }

  def multiplyTFIs(s:Array[Double], d:Array[Double]):Array[Double] = {
    if (s.length > d.length)
    {
      val x = s
      for (y <- 0 to d.length - 1)
        x(y) = s(y) * d(y)
      x
    }
    else
    {
      val x = d
      for (y <- 0 to s.length - 1)
        x(y) = s(y) * d(y)
      x
    }
  }

  def runSemanticSimilarity(t:RDD[(String, Iterable[Double])]):Unit = {
    val temp = t.map(x => (x._1, x._2.toList.distinct))
      .map(x => (x._1, x._2.toArray, x._2.map(y => y * y)))
      .map(x => (x._1, x._2, x._3.sum))
      .map(x => (x._1, x._2, math.sqrt(x._3)))
    val semantics = temp.cartesian(temp)
    semantics.map(x => (x._1._1, x._2._1, multiplyTFIs(x._1._2, x._2._2).sum / (x._1._3 * x._2._3)))
      .sortBy(x => x._3)
      .foreach(x => println(x._1, x._2, x._3))
  }
}