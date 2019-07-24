package ot.geckopipe

import minitest.SimpleTestSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class LocalSparkSessionSuite(sessionName: String, config: SparkConf = new SparkConf) extends SimpleTestSuite {

  def testWithSpark[T](name: String)(f: SparkSession => Unit) {
    def withSpark(tf: SparkSession => Unit) {
      val sparkSession: SparkSession = SparkSession.builder()
        .appName(sessionName)
        .config(config)
        .master("local[*]").getOrCreate()

      try {
        tf(sparkSession)
      } finally {
        sparkSession.close()
      }
    }

    super.test(name)(withSpark(f))
  }
}
