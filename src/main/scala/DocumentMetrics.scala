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
    val tfidf = runCombineTFIDF(tf, idf)
    //runSemanticSimilarity(tfidf)
    runFasterSemantic(tfidf)

  }

  //gets the number of documents in the file
  def setDocumentSize(x:String): Unit =
  {
    documentSize = scala.io.Source.fromFile(x).getLines.size.toDouble
  }
  /*
    gets the number term frequency for every gene in every document
    creates an embedded map
  */
  def runTF(s:RDD[(String, Array[String])]):RDD[(String, String, Double)] = {
    val temp = s.map(value => (value._1, value._2.length))
    val tfi = s.map(f => f._2.map(word => (f._1, word, f._2.filter(gene => gene == word).length.toDouble / f._2.length.toDouble)))
      .flatMap(x => x)
      .map(x => (x._1, x._2, x._3))
    return tfi
  }

  def runIDF(s:RDD[(String, Array[String])], d:Double):RDD[(String, Double)] = {
    val temp = s.map(value => (value._1, value._2.distinct, d))
    val idf = s.map(f => f._2.map(gene => (gene, 1)))
      .flatMap(x => x)
      .reduceByKey(_ + _)
      .map(x => (x._1, math.log(d / (x._2.toDouble))))
      .sortByKey()
    return idf
  }

  def runCombineTFIDF(t:RDD[(String, String, Double)], i:RDD[(String, Double)]):RDD[(String, Iterable[(String, Double)])] = {
    val idf = i.collect().clone()
    val tfidf = t.map(x => (x._1, x._2, idf.filter(y => (x._2 == y._1)).clone(), x._3))
      .map(x=> (x._1, x._2, x._3(0)._2*x._4))
      .map(x => (x._2, (x._1, x._3.toDouble)))
      .groupByKey()
      .map(x => (x._1, x._2.filter(y => y._2!=0)))
      .filter(x => x._2.size != 0 )
    //tfidf.saveAsTextFile("bin/dicks")
    return tfidf
  }

  def multiplyTFIs(s:Array[(String,Double)], d:Array[(String, Double)]): Double = {
    if (s.length > d.length)
    {
      val x = s.distinct.clone().map(x => (x._1, x._2, d.distinct.filter(y => y._1 == x._1).clone()))
        .filter(x => x._3.length != 0)
        .map(x => (x._1, x._2*x._3(0)._2))
        .map(x => x._2)
        .sum

      return x
    }
    else
    {
      val x = d.distinct.clone().map(x => (x._1, x._2, s.distinct.filter(y => y._1 == x._1).clone()))
        .filter(x => x._3.length != 0)
        .map(x => (x._1, x._2*x._3(0)._2))
        .map(x => x._2)
        .sum
      return x

    }
  }

  def computeBrackets(s:Array[(String, Double)]): Double =
  {
    var sum = 0.toDouble
    for(x <- 0 to s.length -1 )
    {
      sum = sum + (s(x)._2 * s(x)._2)
    }
    return math.sqrt(sum)
  }

  def fixRounding(r:Double): Double=
  {
    if(1 < r && r < 2)
      return 1.toDouble
    else
      return r
  }

  def runSemanticSimilarity(t:RDD[(String, Iterable[(String, Double)])]):Unit = {
    val temp = t.map(x => (x._1, x._2.toList, x._2))
    val semantics = temp.cartesian(temp)
      .map(x => (x._1._1, x._2._1, multiplyTFIs(x._1._2.toArray, x._2._2.toArray), computeBrackets(x._1._3.toArray)*computeBrackets(x._2._3.toArray)))
      .map(x => (x._1, x._2, (x._3/x._4)))
      .filter(x => (x._1 != x._2))
      .map(x => (x._1.concat(", " + x._2), x._3))
      .filter(x => x._2!=0)
      .sortBy(x => x._2)

    semantics.foreach(println)
  }

  def runFasterSemantic(t:RDD[(String, Iterable[(String, Double)])]):Unit = {
    val temp = t.map(x => x._2.map(y => (x._1, y._1, y._2)))
      .flatMap(x => x)
      .map(x => (x._1, (x._1, x._2, x._3)))
      .groupByKey()
    val semantics = temp.cartesian(temp)
      .map(x=> ((x._1._1.concat(""), x._1._2), (x._2._1.concat(""), x._2._2)))

    semantics.saveAsTextFile("bin/save")
  }
}
