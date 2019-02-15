package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import ot.geckopipe.index.V2GIndex.logger
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

  /** convert a seq to a stringify array */
  val stringifyColumnString = udf((ls: Seq[String]) => ls.mkString("['", "','", "']"))

  val stringifyColumnDouble = udf((ld: Seq[Double]) => ld.mkString("[", ",", "]"))

  /** convert a seq to a stringify array */
  val stringifyDouble = udf((vs: Seq[String]) => vs match {
    case null => null
    case _    => s"""[${vs.mkString(",")}]"""
  })

  /** save the dataframe as tsv file using filename as a output path */
  def saveToJSON(table: DataFrame, to: String)(implicit sampleFactor: Double = 0d): Unit = {
    logger.info("write datasets to output files")
    if (sampleFactor > 0d) {
      table
        .sample(withReplacement = false, sampleFactor)
        .write.json(to)
    } else {
      table.write.json(to)
    }
  }

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

  def loadFromJSON(uri: String, withSchema: StructType)
                 (implicit ss: SparkSession): DataFrame = ss.read
    .format("json")
    .schema(withSchema)
    .load(uri)

  def loadFromParquet(uri: String)(implicit ss: SparkSession): DataFrame = ss.read.parquet(uri)

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
                     intoColNames: List[String] = VariantIndex.columns,
                     withColTypes: List[String] = VariantIndex.columnsTypes): Try[DataFrame] = {
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
    path.split(usingToken).view.last.split("/").view
      .map(_.toLowerCase).withFilter(_.nonEmpty).take(2).force
  }

  val decileList: Seq[Double] = (10 to 100 by 10).map(_ / 100D)
  /** it maps source_id -> feature -> Seq[(quantile value, quantile number)]
    * gtex_v5 -> whole_blood -> [(0.356, 0.2)]
    */
  /** it needs df contains 3 first columns datasource feature and the vector of doubles */
  def fromQ2Map(df :DataFrame, qs: Seq[Double] = decileList): Map[String, Map[String, Seq[(Double, Double)]]] = {
    var mapQ: Map[String, Map[String, Seq[(Double, Double)]]] = Map.empty

    val rows = df.collect.toList
    val assocs = for (r <- rows) yield {
      val sourceId = r.getAs[String](0)
      val feature = r.getAs[String](1)
      val qs = r.getAs[Seq[Double]](2)
      val qsm: Seq[(Double, Double)] = qs zip decileList
      sourceId -> (feature -> qsm)
    }

    // bizarre reduction to a proper map of maps of lists
    assocs.groupBy(_._1)
      .mapValues(_.map(_._2)
        .groupBy(_._1)
        .mapValues(_.flatMap(_._2)).map(identity))
      .map(identity)
  }

  def computePercentile(ds: DataFrame, tableName: String,
                        scoreField: String, percentileField: String)
                       (implicit ss: SparkSession): DataFrame = {
    logger.info(s"compute quantiles for $scoreField")

    val quantilesDF = ss.sqlContext.sql(
      s"""
         |select
         | source_id,
         | feature,
         | percentile_approx(${scoreField}, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)) as ${percentileField}
         |from ${tableName}
         |group by source_id, feature
         |order by source_id asc, feature asc
      """.stripMargin)

    // build and broadcast qtl and interval maps for the
    val quantiles = ss.sparkContext
      .broadcast(fromQ2Map(quantilesDF))


    val setQuantilesUDF = udf((source_id: String, feature: String, qtl_score: Double) => {
      val qns = quantiles.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < qtl_score).head._2
    })

    val qdf = ds
      .withColumn(percentileField, when(col(scoreField).isNotNull,
        setQuantilesUDF(col("source_id"), col("feature"), col(scoreField))))

    qdf
  }

  object Implicits {
    implicit class ImplicitDataFrameFunctions(df: DataFrame) {
      def withColumnListRenamed(columns: Seq[String], f: String => String): DataFrame = {
        val zippedCols = columns zip columns.map(f(_))
        zippedCols.foldLeft(df)((df, zippedCols) => df.withColumnRenamed(zippedCols._1, zippedCols._2))
      }
    }
  }
}
