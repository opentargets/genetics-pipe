package ot.geckopipe

import minitest.SimpleTestSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class LocalSparkSessionSuite(sessionName: String, config: SparkConf = new SparkConf) extends SimpleTestSuite {
  lazy val sparkSession: SparkSession = SparkSession.builder()
    .appName(sessionName)
    .config(config)
    .master("local[*]").getOrCreate()

  def withSpark[T](f: SparkSession => T): T = {
    try {
      f(sparkSession)
    }
  }
}
