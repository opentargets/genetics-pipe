package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}

/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
class V2GIndex(val table: DataFrame) extends LazyLogging {
  /** compute stats with this resulted table but only when info enabled */
  def computeStats(implicit ss: SparkSession): Seq[Long] = {
    import ss.implicits._
    val totalRows = table.count()
    // val rsidNullsCount = dataset.where($"rsid".isNull).count()
    val inChrCount = table.where($"chr_id".isin(Chromosomes.chrList:_*)).count()

    logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    Seq(inChrCount, totalRows)
  }
}

object V2GIndex extends LazyLogging  {
  trait Component {
    /** unique column name list per component */
    val features: Seq[String]
    val table: DataFrame
  }

  val schema =
    StructType(
      StructField("chr_id", StringType) ::
      StructField("position", LongType) ::
      StructField("ref_allele", StringType) ::
      StructField("alt_allele", StringType) ::
      StructField("gene_id", StringType) ::
      StructField("feature", StringType) ::
      StructField("type_id", StringType) ::
      StructField("source_id", StringType) ::
      StructField("fpred_labels", ArrayType(StringType)) ::
      StructField("fpred_scores", ArrayType(DoubleType)) ::
      StructField("fpred_max_label", StringType) ::
      StructField("fpred_max_score", DoubleType) ::
      StructField("qtl_beta", DoubleType) ::
      StructField("qtl_se", DoubleType) ::
      StructField("qtl_pval", DoubleType) ::
      StructField("qtl_score", DoubleType) ::
      StructField("interval_score", DoubleType) ::
      StructField("qtl_score_q", DoubleType) ::
      StructField("interval_score_q", DoubleType) ::
      StructField("max_qtl", DoubleType) ::
      StructField("max_int", DoubleType) ::
      StructField("max_fpred", DoubleType) ::
      StructField("source_score", DoubleType) ::
      StructField("overall_score", DoubleType) :: Nil)

  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val features: Seq[String] = Seq("feature", "type_id", "source_id")

  /** columns to index the dataset */
  val indexColumns: Seq[String] = Seq("chr_id", "position")
  /** the whole list of columns this dataset will be outputing */
  val columns: Seq[String] = (VariantIndex.columns ++ GeneIndex.idColumns ++ features).distinct

  /** set few columns to NaN and []
    *
    * TODO this needs a bit of refactoring to do it properly
    */
  def fillAndCompute(ds: DataFrame)(implicit ss: SparkSession): DataFrame = {
    // find quantiles (deciles at the moment)
    // get min and max
    ds.createOrReplaceTempView("v2g_table")

    val computedQtlQs = ss.sqlContext.sql(
      """
        |select
        | source_id,
        | feature,
        | percentile_approx(qtl_score, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)) as qtl_score_q
        |from v2g_table
        |where qtl_score is not NULL
        |group by source_id, feature
        |order by source_id asc, feature asc
      """.stripMargin).cache

    val computedIntervalQs = ss.sqlContext.sql(
      """
        |select
        | source_id,
        | feature,
        | percentile_approx(interval_score, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)) as interval_score_q
        |from v2g_table
        |where interval_score is not NULL
        |group by source_id, feature
        |order by source_id asc, feature asc
      """.stripMargin).cache

    val computedNearestQs = ss.sqlContext.sql(
      """
        |select
        | source_id,
        | feature,
        | percentile_approx(inv_d, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)) as distance_score_q
        |from v2g_table
        |where inv_d is not NULL
        |group by source_id, feature
        |order by source_id asc, feature asc
      """.stripMargin).cache

    // build and broadcast qtl and interval maps for the
    val qtlQs = ss.sparkContext
      .broadcast(fromQ2Map(computedQtlQs))
    val intervalQs = ss.sparkContext
      .broadcast(fromQ2Map(computedIntervalQs))

    val nearestsQs = ss.sparkContext
      .broadcast(fromQ2Map(computedNearestQs))

    logger.info(s"compute quantiles for qtls ${qtlQs.value.toString}")
    logger.info(s"compute quantiles for intervals ${intervalQs.value.toString}")
    logger.info(s"compute quantiles for distances ${nearestsQs.value.toString}")

    val setQtlScoreUDF = udf((source_id: String, feature: String, qtl_score: Double) => {
      val qns = qtlQs.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < qtl_score).head._2
    })

    val setIntervalScoreUDF = udf((source_id: String, feature: String, interval_score: Double) => {
      val qns = intervalQs.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < interval_score).head._2
    })

    val setNearestScoreUDF = udf((source_id: String, feature: String, distance_score: Double) => {
      val qns = intervalQs.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < distance_score).head._2
    })

    val qdf = ds
      .withColumn("qtl_score_q", when(col("qtl_score").isNotNull,
        setQtlScoreUDF(col("source_id"), col("feature"), col("qtl_score"))))
      .withColumn("interval_score_q", when(col("interval_score").isNotNull,
        setIntervalScoreUDF(col("source_id"), col("feature"), col("interval_score"))))
      .withColumn("distance_score_q", when(col("inv_d").isNotNull,
        setNearestScoreUDF(col("source_id"), col("feature"), col("inv_d"))))
//      .repartitionByRange(col("chr_id").asc, col("variant_id").asc)
//      .persist(StorageLevel.DISK_ONLY)

    qdf
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def build(datasets: Seq[Component], vIdx: VariantIndex, conf: Configuration)
           (implicit ss: SparkSession): V2GIndex = {

    logger.info("build variant to gene dataset union the list of datasets")

    val allFeatures =
      datasets.tail.foldLeft(datasets.head.features)((agg, el) => agg ++ el.features).distinct

    val processedDts = datasets.map( el =>
      (allFeatures diff el.features).foldLeft(el.table)((agg, el) => agg.withColumn(el, lit(null)))
    )

    val allDts = concatDatasets(processedDts, (columns ++ allFeatures).distinct)
    val postDts = fillAndCompute(allDts)

    new V2GIndex(postDts)
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2GIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2g = ss.read
      .schema(schema)
      .json(conf.variantGene.path)

    new V2GIndex(v2g)
  }
}
