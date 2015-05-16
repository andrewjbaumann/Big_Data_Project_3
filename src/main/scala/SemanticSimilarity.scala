/**
 * Title:           SemanticSimilarity.scala
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
 *                  To run:     scala SparkApp <host> <input_file>
 * Useful Websites: https://spark.apache.org/docs/latest/programming-guide.html
 *                  https://spark.apache.org/docs/latest/configuration.html
 *                  https://spark.apache.org/docs/latest/programming-guide.html
 */

object SparkApp {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkGrep <host> <input_file>")
      System.exit(1)
    }

    val myDocumentMetrics = DocumentMetrics
    myDocumentMetrics.startConfig(args(0), args(1))

    System.exit(0)
  }
}