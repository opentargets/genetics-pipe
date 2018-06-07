package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import scala.util.{Failure, Success, Try}

object Functions extends LazyLogging {
  def splitVariantID(df: DataFrame, variantColName: String = "variant_id",
                     intoColNames: List[String] = variantColumnNames,
                     withColTypes: List[String] = variantColumnTypes): Try[DataFrame] = {
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

  def unwrapInterval(df: DataFrame): DataFrame = {
    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    df.withColumn("position", fromRangeToArray(col("position_start"), col("position_end")))
        .withColumn("position", explode(col("position")))
  }
}
