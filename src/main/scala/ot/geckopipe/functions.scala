package ot.geckopipe

import com.google.common.primitives.UnsignedLong
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import ot.geckopipe.positional._

import scala.util.{Failure, Success, Try}

object functions extends LazyLogging {
  def buildPosSegment(df: DataFrame, fromColumn: String, toColumn: String): DataFrame = {
    val by = 1000L * 1000L
    df.withColumn(toColumn, col(fromColumn).divide(by).cast(LongType))
  }

  def splitVariantID(df: DataFrame, variantColName: String = "variant_id",
                     intoColNames: List[String] = Positional.variantColumnNames,
                     withColTypes: List[String] = Positional.variantColumnTypes): Try[DataFrame] = {
    val variantID = col(variantColName)
    val tmpCol = col("_tmp")

    if (2 to 4 contains intoColNames.length) {
      // get each item from 0 up to las element
      val tmpDF = df.withColumn(tmpCol.toString, split(variantID, "_"))

      val modDF = intoColNames.zipWithIndex.foldLeft(tmpDF)( (df, pair) => {
        df.withColumn(pair._1, tmpCol.getItem(pair._2).cast(withColTypes(pair._2)))
      }).drop(tmpCol)

      Success(modDF)

    } else
      Failure(new IllegalArgumentException("You need >= 2 columns in order to split a variant_id -> chr, pos"))
  }

  def concatDatasets(datasets: Seq[DataFrame], columns: List[String]): Option[DataFrame] = datasets match {
    case Nil => None
    case _ =>
      logger.info("build variant to gene dataset union the list of datasets")
      val dts = datasets.foldLeft(datasets.head.select(columns.head, columns.tail: _*))((aggDt, dt) => {
        aggDt.union(dt.select(columns.head, columns.tail: _*))
      })

      Some(dts)
  }
}
