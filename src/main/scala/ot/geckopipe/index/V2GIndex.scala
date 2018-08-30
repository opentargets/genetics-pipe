package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}

/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
abstract class V2GIndex extends Indexable {
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

  val fullSchema =
    StructType(
      StructField("chr_id", StringType) ::
      StructField("position", LongType) ::
      StructField("ref_allele", StringType) ::
      StructField("alt_allele", StringType) ::
      StructField("variant_id", StringType) ::
      StructField("rs_id", StringType) ::
      StructField("gene_chr", StringType) ::
      StructField("gene_id", StringType) ::
      StructField("gene_start", LongType) ::
      StructField("gene_stop", LongType) ::
      StructField("gene_type", StringType) ::
      StructField("gene_name", StringType) ::
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
  val columns: Seq[String] = (VariantIndex.columns ++ EnsemblIndex.columns ++ features).distinct

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

    // show all quantiles
    computedQtlQs.show(500, false)
    computedIntervalQs.show(500, false)

    // build and broadcast qtl and interval maps for the
    val qtlQs = ss.sparkContext
      .broadcast(fromQ2Map(computedQtlQs))
    val intervalQs = ss.sparkContext
      .broadcast(fromQ2Map(computedIntervalQs))

    logger.info(s"compute quantiles for qtls ${qtlQs.value.toString}")
    logger.info(s"compute quantiles for intervals ${intervalQs.value.toString}")

    val setQtlScoreUDF = udf((source_id: String, feature: String, qtl_score: Double) => {
      val qns = qtlQs.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < qtl_score).head._2
    })

    val setIntervalScoreUDF = udf((source_id: String, feature: String, interval_score: Double) => {
      val qns = intervalQs.value.apply(source_id).apply(feature)
      qns.view.dropWhile(p => p._1 < interval_score).head._2
    })

    val qdf = ds
      .withColumn("qtl_score_q", when(col("qtl_score").isNotNull,
        setQtlScoreUDF(col("source_id"), col("feature"), col("qtl_score"))))
      .withColumn("interval_score_q", when(col("interval_score").isNotNull,
        setIntervalScoreUDF(col("source_id"), col("feature"), col("interval_score"))))
      .persist(StorageLevel.DISK_ONLY)

    qdf.createOrReplaceTempView("v2g_table")

    val perSourceScore = ss.sqlContext.sql(
      """
        |select
        | chr_id,
        | variant_id,
        | gene_id,
        | source_id,
        | max(ifNull(qtl_score_q, 0.)) AS max_qtl,
        | max(ifNull(interval_score_q, 0.)) AS max_int,
        | max(ifNull(fpred_max_score, 0.)) AS max_fpred,
        | max(ifNull(qtl_score_q, 0.)) + max(ifNull(interval_score_q, 0.)) + max(ifNull(fpred_max_score, 0.)) AS source_score
        |from v2g_table
        |group by chr_id, variant_id, gene_id, source_id
      """.stripMargin)

    perSourceScore.createOrReplaceTempView("v2g_table_sscore")

    val overAllScores = ss.sqlContext.sql(
      """
        |select
        | chr_id,
        | variant_id,
        | gene_id,
        | avg(source_score) AS overall_score
        |from v2g_table_sscore
        |group by chr_id, variant_id, gene_id
      """.stripMargin)

    val sdf = perSourceScore.join(overAllScores,
      Seq("chr_id", "variant_id", "gene_id"))
      .toDF("chr_id1", "variant_id1", "gene_id1", "source_id1", "max_qtl",
        "max_int", "max_fpred", "source_score", "overall_score")
      .persist(StorageLevel.DISK_ONLY)

    // jointScoresTable.show(10, false)
    // dsWithQs.show(10, false)

    // thanks to stackoverflow
    // https://stackoverflow.com/questions/51676083/java-spark-spark-bug-workaround-for-datasets-joining-with-unknow-join-column-n
    // https://issues.apache.org/jira/browse/SPARK-14948
    // toDF and change the column name
    val dsAggregated = qdf.join(sdf,qdf.col("chr_id") === sdf.col("chr_id1") and
      qdf.col("variant_id") === sdf.col("variant_id1") and
      qdf.col("gene_id") === sdf.col("gene_id1") and
      qdf.col("source_id") === sdf.col("source_id1"))

    dsAggregated
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def build(datasets: Seq[Component], vIdx: VariantIndex, conf: Configuration)
           (implicit ss: SparkSession): V2GIndex = {

    logger.info("build variant to gene dataset union the list of datasets")
    logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
    val geneTrans = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
      .aggByGene
      .cache

    val allFeatures = datasets.foldLeft(datasets.head.features)((agg, el) => agg ++ el.features).distinct

    val processedDts = datasets.map( el => {
      val table = (allFeatures diff el.features).foldLeft(el.table)((agg, el) => agg.withColumn(el, lit(null)))
      table.join(geneTrans, Seq("gene_id"))
    })

    val allDts = concatDatasets(processedDts, (columns ++ allFeatures).distinct)
    val postDts = fillAndCompute(allDts)

    new V2GIndex {
      override val table: DataFrame = postDts
    }
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2GIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2g = ss.read
      .schema(fullSchema)
      .json(conf.variantGene.path)

    new V2GIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = v2g
    }
  }
}
