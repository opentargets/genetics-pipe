package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.EnsemblIndex
import ot.geckopipe.functions._

import scala.collection.mutable

object VEP extends LazyLogging {
  val features: Seq[String] = Seq("fpred_labels", "fpred_scores")

  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("variant_pos", LongType) ::
      StructField("rs_id", StringType) ::
      StructField("ref_allele", StringType) ::
      StructField("alt_allele", StringType) ::
      StructField("qual", StringType) ::
      StructField("filter", StringType) ::
      StructField("info", StringType) :: Nil)

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

  def loadHumanVEP(from: String)(implicit ss: SparkSession): DataFrame = {
    val vepss = ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("comment", "\u0023")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)
      .withColumn("alt_allele",split(col("alt_allele"), ","))
      .withColumn("alt_allele",explode(col("alt_allele")))
      .withColumn("variant_id",
        concat_ws("_", col("chr_id"), col("variant_pos"),
          col("ref_allele"), col("alt_allele")))
        .withColumnRenamed("variant_pos", "position")

    vepss
  }


  /** join gene id per extracted transcript (it should be one per row)
    *
    * generate variant_id column
    * drop not needed ones
    * rename geneID to gene_id in order to keep names equal
    * filter out those with no gene_id
    * repartition based on variant_id and gene_id
    * and persist if you want to keep the partition through next operations (by ex. joins)
    * generate pivot per consecuence and set to count or fill with 0
    */
  def apply(conf: Configuration)(implicit ss: SparkSession): Component = {
    import ss.implicits._

    logger.info("load and cache ensembl gene to transcript LUT getting only gene_id and trans_id")
    val geneTrans = EnsemblIndex(conf.ensembl.geneTranscriptPairs).table
      .select("gene_id", "gene_chr", "trans_id")
      .cache

    // from csqs table to a map to broadcast to all workers
    val csqsMap = loadConsequenceTable(conf.vep.homoSapiensConsScores)
      .select("term", "v2g_score")
      .map(r => (r.getAs[String](0), r.getAs[Double](1)))
      .collect
      .toMap
    // broadcast the small Map to be used in each worker as it is loaded into memmory
    val csqScoresBc = ss.sparkContext.broadcast(csqsMap)

    println(csqsMap.toString)

    val udfCsqScores = udf((csqs: mutable.WrappedArray[String]) => {
      csqs.map(csqScoresBc.value.getOrElse(_, 0.0))
    })

    // split info string and extract CSQ substring
    // it returns a list of consequences
    val udfCSQ = udf( (info: String) => {
      val csql = info.split(";")
        .filter(_.startsWith("CSQ="))
        .flatMap(_.stripPrefix("CSQ=").split(","))

      csql
    })

    val udfTSA = udf( (info: String) => {
      val tsa = info.split(";")
        .filter(_.startsWith("TSA="))
        .map(_.stripPrefix("TSA="))

      tsa(0)
    })

    val filterCSQByAltAllele = udf( (refAllele: String, altAllele: String, tsa: String, csqs: Seq[String]) => {
      tsa match {
        case s if Set("SNV", "insertion", "substitution").contains(s) =>
          if (altAllele.startsWith(refAllele))
            csqs.filter(_.startsWith(altAllele.stripPrefix(refAllele)))
          else
            csqs.filter(_.startsWith(altAllele))
        case _ =>
          csqs
      }
    })

    logger.info("load VEP table for homo sapiens")
    val veps = loadHumanVEP(conf.vep.homoSapiensCons)
      .withColumn("tsa", udfTSA($"info"))
      .withColumn("csq", udfCSQ($"info"))
      .withColumn("csq", filterCSQByAltAllele($"ref_allele", $"alt_allele", $"tsa", $"csq"))
      .withColumn("csq", explode($"csq"))
      .withColumn("csq", split($"csq", "\\|"))
      .withColumn("consequence", $"csq".getItem(1))
      .withColumn("trans_id", $"csq".getItem(3))
      .drop("qual", "filter", "info", "tsa")

    logger.info("inner join vep consequences transcripts to genes")
    val vepsDF = veps.join(geneTrans, Seq("trans_id"), "left_outer")
      .where($"gene_id".isNotNull and $"chr_id" === $"gene_chr")
      .drop("trans_id", "csq", "tss_distance", "gene_chr")
      .groupBy("variant_id", "gene_id")
      .agg(collect_set("consequence").as("consequence_set"),
        first("chr_id").as("chr_id"),
        first("position").as("position"),
        first("ref_allele").as("ref_allele"),
        first("alt_allele").as("alt_allele"),
        first("rs_id").as("rs_id"))
      .withColumn("type_id", lit("fpred"))
      .withColumn("source_id", lit("vep"))
      .withColumn("feature", lit("unspecified"))
      .withColumn("fpred_scores", udfCsqScores(col("consequence_set")))
      .withColumnRenamed("consequence_set", "fpred_labels")
      .withColumn("fpred_labels", stringifyColumnString(col("fpred_labels")))
      .withColumn("fpred_scores", stringifyColumnDouble(col("fpred_scores")))

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = VEP.features
      override val table: DataFrame = vepsDF
    }
  }
}
