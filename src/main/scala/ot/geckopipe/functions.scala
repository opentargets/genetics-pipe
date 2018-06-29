package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import ot.geckopipe.index.VariantIndex

import scala.util.{Failure, Success, Try}

object functions extends LazyLogging {
  def addSourceID(df: DataFrame, column: Column): DataFrame = df.withColumn("source_id", column)

  def buildPosSegment(df: DataFrame, fromColumn: String, toColumn: String): DataFrame = {
    val by = 1000L * 1000L
    df.withColumn(toColumn, col(fromColumn).divide(by).cast(LongType))
  }

  def splitVariantID(df: DataFrame, variantColName: String = "variant_id",
                     intoColNames: List[String] = VariantIndex.variantColumnNames,
                     withColTypes: List[String] = VariantIndex.variantColumnTypes): Try[DataFrame] = {
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

  def concatDatasets(datasets: Seq[DataFrame], columns: Seq[String]): DataFrame = {
    logger.info("build variant to gene dataset union the list of datasets")
    val dts = datasets.foldLeft(datasets.head.select(columns.head, columns.tail: _*))((aggDt, dt) => {
      aggDt.union(dt.select(columns.head, columns.tail: _*))
    })

    dts
  }

  /** split by usingToken and grab the right side of the string path. Then split by '/'. It returns
    * .../type_name/source_name/tissue_name/... (type_name, source_name, tissue_name) all lowercased
    *
    * @param path input path to split by usingToken and then by '/'
    * @param usingToken the token used to partition the input path
    * @return the triplet of (type_name, source_nmae, tissue_name)
    */
  def extractValidTokensFromPath(path: String, usingToken: String): Array[String] = {
    val validTokens = path.split(usingToken).last.split("/").filter(_.nonEmpty)
    val tokenList = Array(validTokens.head.toLowerCase,
      validTokens.tail.take(1).head.toLowerCase,
      validTokens.tail.drop(1).head.toLowerCase)

    tokenList
  }
}
