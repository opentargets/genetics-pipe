package ot.geckopipe

import java.util.UUID

import minitest.SimpleTestSuite
import org.apache.spark.sql.SparkSession
import ot.geckopipe.domain._

object DataProcessingSuite extends SimpleTestSuite {

  private val configuration = createTestConfiguration()
  private implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  val error = 1E-3

  import spark.implicits._

  Seq(
    RawVariant("1", 1000, "1", 1100, "A", "T", "rs123",
      Vep(most_severe_consequence = "severe consequence",
        transcript_consequences = Array(
          TranscriptConsequence(gene_id = "gene id", consequence_terms = Array("consequence term 1")))),
      "cadd 1", "af 1")
  ).toDF().write.parquet(configuration.variantIndex.raw)
  Seq(
    Gene("1", "ENSG00000223972", 11869, 11869, 14412, "protein_coding")
  ).toDF().write.json(configuration.ensembl.lut)
  Seq(
    VepConsequence(
      "http://purl.obolibrary.org/obo/SO_000163",
      "upstream_gene_variant",
      "A sequence variant located 5' of a gene",
      "Upstream gene variant",
      "MODIFIER",
      0.0,
      0.6
    )
  ).toDF().write.json(configuration.vep.homoSapiensConsScores)
  Seq(
    Qtl("1", 1100L, "A", "T", "ENSG00000223972", 0.1, 0.2, 0.3, "type 1", "src 1", "feature 1")
  ).toDF().write.parquet(configuration.qtl.path)
  Seq(
    IntervalDomain("1", 900L, 1200L, "ENSG00000223972", 0.1, "cell type 1", "feature 1")
  ).toDF().write.parquet(configuration.interval.path)

  test("calculate variant index") {
    Main.run(CommandLineArgs(command = Some("variant-index")), configuration)

    def variants = spark.read.parquet(configuration.variantIndex.path).as[Variant].collect().toList

    assertEquals(variants, List(
      Variant("1", 1100, "1", 1000, "A", "T", "rs123", "severe consequence", "cadd 1", "af 1", 10769L, "ENSG00000223972",
        10769L, "ENSG00000223972")
    ))
  }

  test("calculate distance nearest") {
    Main.run(CommandLineArgs(command = Some("distance-nearest")), configuration)

    def nearest = spark.read.json(configuration.nearest.path).as[Nearest].collect()

    assertEquals(nearest.length, 1)
    val firstNearest = nearest(0)
    assertEquals(firstNearest.chr_id, "1")
    assertEquals(firstNearest.position, 1100L)
    assertEquals(firstNearest.ref_allele, "A")
    assertEquals(firstNearest.alt_allele, "T")
    assertEquals(firstNearest.gene_id, "ENSG00000223972")
    assertEquals(firstNearest.d, 10769L)
    assert(firstNearest.distance_score - 9.285 < error)
    assert(firstNearest.distance_score_q - 0.1 < error)
    assertEquals(firstNearest.type_id, "distance")
    assertEquals(firstNearest.source_id, "canonical_tss")
    assertEquals(firstNearest.feature, "unspecified")
  }

  test("calculate variant to gene") {
    Main.run(CommandLineArgs(command = Some("variant-gene")), configuration)

    def v2gs = spark.read.json(configuration.variantGene.path).as[V2G].collect()

    assertEquals(v2gs.length, 3)

    assert(v2gs.forall(_.chr_id == "1"))
    assert(v2gs.forall(_.position == 1100L))
    assert(v2gs.forall(_.ref_allele == "A"))
    assert(v2gs.forall(_.alt_allele == "T"))
    assert(v2gs.forall(_.gene_id == "ENSG00000223972"))

    val v2gsDistance = v2gs.find(_.type_id == "distance").get
    assertEquals(v2gsDistance.feature, "unspecified")
    assertEquals(v2gsDistance.source_id, "canonical_tss")
    assertEquals(v2gsDistance.d, Some(10769L))
    assert(v2gsDistance.distance_score.get - 9.285913269570062E-5 < 1E-8)
    assert(v2gsDistance.distance_score_q.get - 0.1 < error)

    val v2gsType1 = v2gs.find(_.type_id == "type 1").get
    assertEquals(v2gsType1.feature, "feature 1")
    assertEquals(v2gsType1.source_id, "src 1")
    assert(v2gsType1.qtl_beta.get - 0.1 < error)
    assert(v2gsType1.qtl_se.get - 0.2 < error)
    assert(v2gsType1.qtl_pval.get - 0.3 < error)
    assert(v2gsType1.qtl_score.get - 0.523 < error)
    assert(v2gsType1.qtl_score_q.get - 0.1 < error)

    val v2gsAsterisk = v2gs.find(_.type_id == "*").get
    assertEquals(v2gsAsterisk.feature, "feature 1")
    assertEquals(v2gsAsterisk.source_id, "*")
    assert(v2gsAsterisk.interval_score.get - 0.1 < error)
    assert(v2gsAsterisk.interval_score_q.get - 0.1 < error)
  }

  private def createTestConfiguration(): Configuration = {
    val uuid = UUID.randomUUID().toString

    val testDataFolder = s"/tmp/tests-$uuid"
    val inputFolder = s"$testDataFolder/input"
    val outputFolder = s"$testDataFolder/output"

    Configuration(
      output = outputFolder,
      sampleFactor = 0, //disabled
      sparkUri = "", //empty string for local
      logLevel = "INFO",
      ensembl = EnsemblSection(lut = s"$inputFolder/hg38.json"),
      vep = VEPSection(homoSapiensConsScores = s"$inputFolder/vep_consequences.tsv"),
      interval = IntervalSection(path = s"$inputFolder/v2g/interval/*/*/data.parquet/"),
      qtl = QTLSection(path = s"$inputFolder/v2g/qtl/*/*/data.parquet/"),
      nearest = NearestSection(tssDistance = 500000, path = s"$outputFolder/distance/canonical_tss/"),
      variantIndex =
        VariantSection(
          raw = s"$inputFolder/variant-annotation.parquet/",
          path = s"$outputFolder/variant-index/",
          tssDistance = 500000),
      variantGene = VariantGeneSection(path = s"$outputFolder/v2g/"),
      variantDisease =
        VariantDiseaseSection(
          path = s"$outputFolder/v2d/",
          studies = s"$inputFolder/v2d/studies.parquet",
          toploci = s"$inputFolder/v2d/toploci.parquet",
          finemapping = s"$inputFolder/v2d/finemapping.parquet",
          ld = s"$inputFolder/v2d/ld.parquet",
          overlapping = s"$inputFolder/v2d/locus_overlap.parquet",
          coloc = s"$inputFolder/coloc/010101/"))
  }
}
