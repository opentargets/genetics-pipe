package ot.geckopipe

import org.apache.spark.{SparkConf, SparkContext}

trait LocalSparkContext {
  def withSpark[T](f: SparkContext => T): T = {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "gecko-pipe-test", conf)

    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}
