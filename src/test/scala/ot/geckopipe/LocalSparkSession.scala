package ot.geckopipe

import minitest.SimpleTestSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class LocalSparkSessionSuite(sessionName: String, config: SparkConf = new SparkConf) extends SimpleTestSuite {

  def withSpark[T](f: SparkSession => T): T = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName(sessionName)
      .config(config)
      .master("local[*]").getOrCreate()

    try {
      f(sparkSession)
    } finally {
      sparkSession.close()
    }
  }
}
