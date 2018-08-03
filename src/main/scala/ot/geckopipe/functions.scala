package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import ot.geckopipe.index.V2DIndex.studiesSchema
import ot.geckopipe.index.VariantIndex

import scala.util.{Failure, Success, Try}

object functions extends LazyLogging {
  /** chromosomes is a map with default 0 that returns a Int from 1 to 25
    * from a chromosome string 1 to 22 and X Y MT consecutively numbered
    */
  val chromosomes: Map[String, Int] = {
    val chrs = (1 to 22).map(_.toString) ++ Seq("X", "Y", "MT")
    val chrsToIDs = (chrs zip (1 to 25)).toMap withDefaultValue 0
    chrsToIDs
  }

  val chromosomesUDF = udf((chr_id: String) => chromosomes(chr_id))

  /** split two columns by ; each column and then zip and map to an array */
  val splitAndZip = udf((codes: String, labels: String) => codes.split(";")
    .zipAll(labels.split(";"),"", "")
    .filter(_._1 != "")
    .map(t => Array(t._1,t._2)))

  val arrayToString = udf((xs: Array[String]) => xs.mkString("[",",","]"))

  /** save the dataframe as tsv file using filename as a output path */
  def saveToCSV(table: DataFrame, to: String)(implicit sampleFactor: Double = 0d): Unit = {
    logger.info("write datasets to output files")
    if (sampleFactor > 0d) {
      table
        .sample(withReplacement = false, sampleFactor)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", "\t")
        .save(to)
    } else {
      table
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", "\t")
        .save(to)
    }
  }

  def loadFromCSV(uri: String, withSchema: StructType, andHeader: Boolean = true)
                 (implicit ss: SparkSession): DataFrame = ss.read
    .format("csv")
    .option("header", andHeader.toString)
    .option("delimiter","\t")
    .schema(withSchema)
    .load(uri)

  def addSourceID(df: DataFrame, column: Column): DataFrame = df.withColumn("source_id", column)

  def buildPosSegment(df: DataFrame, fromColumn: String, toColumn: String): DataFrame = {
    val by = 1000L * 1000L
    df.withColumn(toColumn, col(fromColumn).divide(by).cast(LongType))
  }

  def splitVariantID(df: DataFrame, variantColName: String = "variant_id",
                     prefix: String = "",
                     intoColNames: List[String] = VariantIndex.variantColumnNames,
                     withColTypes: List[String] = VariantIndex.variantColumnTypes): Try[DataFrame] = {
    val variantID = col(variantColName)
    val tmpCol = col("_tmp")

    if (2 to 4 contains intoColNames.length) {
      // get each item from 0 up to las element
      val tmpDF = df.withColumn(tmpCol.toString, split(variantID, "_"))

      val modDF = intoColNames.zipWithIndex.foldLeft(tmpDF)( (df, pair) => {
        df.withColumn(prefix + pair._1, tmpCol.getItem(pair._2).cast(withColTypes(pair._2)))
      }).drop(tmpCol)

      Success(modDF)

    } else
      Failure(new IllegalArgumentException("You need >= 2 columns in order to split a variant_id -> chr, pos"))
  }

  def concatDatasets(datasets: Seq[DataFrame], columns: Seq[String]): DataFrame = {
    logger.info("build variant to gene dataset union the list of datasets")
    val dts = datasets.tail.foldLeft(datasets.head.select(columns.head, columns.tail: _*))((aggDt, dt) => {
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
