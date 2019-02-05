package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.{GeneIndex, VariantIndex}
import ot.geckopipe.index.Indexable._

import scala.collection.mutable

object VEP extends LazyLogging {
  val features: Seq[String] = Seq("fpred_labels", "fpred_scores", "fpred_max_label", "fpred_max_score")
  val columns: Seq[String] =
    Seq("chr_id", "position", "ref_allele", "alt_allele", "gene_id") ++ features

  val rawColumnsWithAliases: Seq[(String, String)] = Seq(("chrom_b37","chr_id"), ("pos_b37", "position"),
    ("ref", "ref_allele"), ("alt", "alt_allele"), ("rsid", "rs_id"),
    ("vep.transcript_consequences", "transcript_consequences"))

  /** load consequence table from file extracted from ensembl website
    *
    * https://www.ensembl.org/info/genome/variation/predicted_data.html#consequences and
    * merged with OT eco scores table. We filter by only v2g_scores and get last token from
    * the accession terms
    *
    * @param from file to load the lookup table
    * @param ss the implicit sparksession
    * @return a dataframe with all normalised columns
    */
  def loadConsequenceTable(from: String)(implicit ss: SparkSession): DataFrame = {
    val cleanAccessions = udf((accession: String) => {
      accession.split("/").lastOption.getOrElse(accession)
    })

    val csqSchema = StructType(
      StructField("accession", StringType) ::
      StructField("term", StringType) ::
        StructField("description", StringType) ::
        StructField("display_term", StringType) ::
        StructField("impact", StringType) ::
        StructField("v2g_score", DoubleType) ::
        StructField("eco_score", DoubleType) :: Nil)

    val csqs = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("mode", "DROPMALFORMED")
      .schema(csqSchema)
      .load(from)

    csqs
      .withColumn("accession", cleanAccessions(col("accession")))
      .filter(col("v2g_score").isNotNull)
  }

  def apply(conf: Configuration)(implicit ss: SparkSession): Component = {
    import ss.implicits._

    val genes = GeneIndex(conf.ensembl.lut)
      .sortByID
      .table.selectBy(GeneIndex.indexColumns :+ "gene_id")
      .cache()

    // from csqs table to a map to broadcast to all workers
    val csqsMap = loadConsequenceTable(conf.vep.homoSapiensConsScores)
      .select("term", "v2g_score")
      .map(r => (r.getAs[String](0), r.getAs[Double](1)))
      .collect
      .toMap

    // broadcast the small Map to be used in each worker as it is loaded into memory
    val csqScoresBc = ss.sparkContext.broadcast(csqsMap)

    val udfCsqScores = udf((csqs: mutable.WrappedArray[String]) => {
      csqs.map(csqScoresBc.value.getOrElse(_, 0.0))
    })

    // return the max pair with label and score from the two lists of labels with scores
    val getMaxCsqLabel = udf( (labels: Seq[String], scores: Seq[Double]) =>
      (labels zip scores).sortBy(_._2)(Ordering[Double].reverse).head._1
    )

    val getMaxCsqScore = udf( (labels: Seq[String], scores: Seq[Double]) =>
      (labels zip scores).sortBy(_._2)(Ordering[Double].reverse).head._2
    )

    val getMaxLabel = udf((p: (String, Double)) => p._1)
    val getMaxScore = udf((p: (String, Double)) => p._2)

    logger.info("load VEP table from raw variant index")
    val raw = VariantIndex
      .builder(conf)
      .loadRawVariantIndex(rawColumnsWithAliases)
      .persist(StorageLevels.DISK_ONLY)

    val veps = raw.where(col("transcript_consequences").isNotNull)

    // TODO finish the VEP
    veps.show(false)
      // .withColumn("consequence_set",col())

//    logger.info("load VEP table for homo sapiens")
//    val veps = loadHumanVEP(conf.vep.homoSapiensCons)
//      .withColumn("tsa", udfTSA($"info"))
//      .withColumn("csq", udfCSQ($"info"))
//      .withColumn("csq", filterCSQByAltAllele($"ref_allele", $"alt_allele", $"tsa", $"csq"))
//      .withColumn("csq", explode($"csq"))
//      .withColumn("csq", split($"csq", "\\|"))
//      .withColumn("consequence", $"csq".getItem(1))
//      .withColumn("trans_id", $"csq".getItem(3))
//      .drop("qual", "filter", "info", "tsa")
//
//    logger.info("inner join vep consequences transcripts to genes")
//    val vepsDF = veps.join(geneTrans, Seq("trans_id"), "left_outer")
//      .where($"gene_id".isNotNull and $"chr_id" === $"gene_chr")
//      .drop("trans_id", "csq", "tss_distance", "gene_chr")
//      .groupBy("variant_id", "gene_id")
//      .agg(collect_set("consequence").as("consequence_set"),
//        first("chr_id").as("chr_id"),
//        first("position").as("position"),
//        first("ref_allele").as("ref_allele"),
//        first("alt_allele").as("alt_allele"),
//        first("rs_id").as("rs_id"))
//      .withColumn("type_id", lit("fpred"))
//      .withColumn("source_id", lit("vep"))
//      .withColumn("feature", lit("unspecified"))
//      .withColumn("fpred_scores", udfCsqScores(col("consequence_set")))
//      .withColumnRenamed("consequence_set", "fpred_labels")
//      .withColumn("fpred_max_label", getMaxCsqLabel(col("fpred_labels"), col("fpred_scores")))
//      .withColumn("fpred_max_score", getMaxCsqScore(col("fpred_labels"), col("fpred_scores")))
//      .where(col("fpred_max_score") > 0F)

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = VEP.features
      override val table: DataFrame = veps
    }
  }
}
