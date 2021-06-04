package ot.geckopipe

import minitest.SimpleTestSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class LocalSparkSessionSuite(sessionName: String, config: Option[SparkConf] = None)
    extends SimpleTestSuite {

  private[this] lazy val defaultSparkConf: SparkConf = new SparkConf().setMaster("local[*]")

  implicit private[this] lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(sessionName)
    .config(config.getOrElse(defaultSparkConf))
    .getOrCreate()

  def testWithSpark[T](name: String)(f: SparkSession => Unit) {
    super.test(name)(f(sparkSession))
  }
}
