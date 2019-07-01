package ot.geckopipe

import java.util.UUID

import minitest.SimpleTestSuite
import org.apache.spark.sql.{Encoders, SparkSession}
import ot.geckopipe.domain._

object DataProcessingSuite extends SimpleTestSuite {

  private val configuration = createTestConfiguration()
  private implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  val error = 1E-3

  import spark.implicits._

  private val variant = Variant("1", 1100L, "A", "T", Some("rs123"), Some("severe consequence"), Some(0.1),
    Some(0.2), Some(0.1), Some(0.2), Some(0.3), Some(0.4), Some(0.5), Some(0.6), Some(0.7), Some(0.8), Some(0.9),
    Some(1.0), Some(10769L), Some("ENSG00000223972"), Some(10769L), Some("ENSG00000223972"))
  Seq(
    RawVariant("1", 1000, "1", 1100, "A", "T", "rs123",
      Vep(most_severe_consequence = "severe consequence",
        transcript_consequences = Array(
          TranscriptConsequence(gene_id = "gene id", consequence_terms = Array("consequence term 1")))),
      Cadd(0.1, 0.2), Gnomad(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0))
  ).toDF().write.parquet(configuration.variantIndex.raw)

  private val gene = Gene("1", "ENSG00000223972", "DDX11L1",
    "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1 [Source:HGNC Symbol;Acc:37102]", fwdstrand = true,
    exons = Seq(11869L, 12227L, 12613L, 12721L, 13221L, 14409L), 11869, 11869, 14412, "protein_coding")
  Seq(gene).toDF().write.json(configuration.ensembl.lut)

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

  private val study = Study("study1", "trait reported 1", List("EFO_test"), Some("PMID:1"), Some("2012-01-03"),
    Some("journal 1"), Some("pub title 1"), Some("Pub Author"), List("European=10"), List("European=5"),
    Some(10L), Some(5L), Some(1L), Some("trait category 1"), Some(2L))
  Seq(study).toDF().write.parquet(configuration.variantDisease.studies)

  Seq(
    TopLoci("study1", "1", 1100L, "A", "T", Some("+"), Some(0.026), Some(0.021), Some(0.030), Some(0.11), Some(0.09),
      Some(0.12), Some(2.3), Some(-16))
  ).toDF().write.parquet(configuration.variantDisease.toploci)

  Seq(
    Ld("study1", "1", 1100L, "A", "T", "1", 1100L, "A", "T", Some(0.9), Some(0.1), Some(0.2), Some(0.3), Some(0.4),
      Some(0.5), Some(true))
  ).toDF().write.parquet(configuration.variantDisease.ld)

  Seq(
    FineMapping("study1", "1", 1100L, "A", "T", "1", 1100L, "A", "T", Some(28.9), Some(0.021))
  ).toDF().write.parquet(configuration.variantDisease.finemapping)

  private val overlap = LocusOverlap("study1", "1", 1100L, "A", "T", "study2", "1", 1100L, "A", "T", 7, 3, 5)
  Seq(overlap).toDF().write.parquet(configuration.variantDisease.overlapping)

  test("calculate variant index") {
    Main.run(CommandLineArgs(command = Some("variant-index")), configuration)

    def variants = spark.read.schema(Encoders.product[Variant].schema).
      parquet(configuration.variantIndex.path).as[Variant].collect().toList

    assertEquals(variants, List(Variant("1", 1100L, "A", "T", Some("rs123"), Some("severe consequence"),
      gene_id_any_distance = Some(10769L),
      gene_id_any = Some("ENSG00000223972"),
      gene_id_prot_coding_distance = Some(10769L),
      gene_id_prot_coding = Some("ENSG00000223972"))))
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

    def v2gs = spark.read.schema(Encoders.product[V2G].schema).
      json(configuration.variantGene.path).as[V2G].collect()

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

  test("calculate variant to disease") {
    Main.run(CommandLineArgs(command = Some("variant-disease")), configuration)

    def v2d = spark.read.json(configuration.variantDisease.path).as[V2D].head()

    assertEquals(v2d.study_id, "study1")
    assertEquals(v2d.pmid, Some("PMID:1"))
    assertEquals(v2d.pub_date, Some("2012-01-03"))
    assertEquals(v2d.pub_journal, Some("journal 1"))
    assertEquals(v2d.pub_author, Some("Pub Author"))
    assertEquals(v2d.trait_reported, "trait reported 1")
    assertEquals(v2d.trait_efos, List("EFO_test"))
    assertEquals(v2d.ancestry_initial, List("European=10"))
    assertEquals(v2d.ancestry_replication, List("European=5"))
    assertEquals(v2d.n_initial, Some(10))
    assertEquals(v2d.n_replication, Some(5))
    assertEquals(v2d.n_cases, Some(1))
    assertEquals(v2d.trait_category, Some("trait category 1"))
    assertEquals(v2d.num_assoc_loci, Some(2))
    assertEquals(v2d.lead_chrom, "1")
    assertEquals(v2d.lead_pos, 1100L)
    assertEquals(v2d.lead_ref, "A")
    assertEquals(v2d.lead_alt, "T")
    assertEquals(v2d.tag_chrom, "1")
    assertEquals(v2d.tag_pos, 1100L)
    assertEquals(v2d.tag_ref, "A")
    assertEquals(v2d.tag_alt, "T")
    assert(v2d.overall_r2.get - 0.9 < error)
    assert(v2d.AFR_1000G_prop.get - 0.1 < error)
    assert(v2d.AMR_1000G_prop.get - 0.2 < error)
    assert(v2d.EAS_1000G_prop.get - 0.3 < error)
    assert(v2d.EUR_1000G_prop.get - 0.4 < error)
    assert(v2d.SAS_1000G_prop.get - 0.5 < error)
    assert(v2d.log10_ABF.get - 28.9 < error)
    assert(v2d.posterior_prob.get - 0.021 < error)
    assert(v2d.odds_ratio.get - 0.11 < error)
    assert(v2d.oddsr_ci_lower.get - 0.09 < error)
    assert(v2d.oddsr_ci_upper.get - 0.12 < error)
    assertEquals(v2d.direction, Some("+"))
    assert(v2d.beta.get - 0.026 < error)
    assert(v2d.beta_ci_lower.get - 0.021 < error)
    assert(v2d.beta_ci_upper.get - 0.03 < error)
    assert(v2d.pval_mantissa - 2.3 < error)
    assertEquals(v2d.pval_exponent, -16)
    assert(v2d.pval - 2.3 - 16 < 1E-19)
  }

  test("calculate disease to variant to gene") {
    Main.run(CommandLineArgs(command = Some("disease-variant-gene")), configuration)

    def d2v2gs = spark.read.schema(Encoders.product[D2V2G].schema).
      json(configuration.output + "/d2v2g/").as[D2V2G].collect()

    assertEquals(d2v2gs.length, 3)
  }

  test("calculate dictionaries") {
    Main.run(CommandLineArgs(command = Some("dictionaries")), configuration)

    def genes = spark.read.json(configuration.output + "/lut/genes-index/").as[Gene].collect().toList
    assertEquals(genes, List(gene))

    def studies = spark.read.json(configuration.output + "/lut/study-index/").as[Study].collect().toList
    assertEquals(studies, List(study))

    def variants = spark.read.json(configuration.output + "/lut/variant-index/").as[Variant].collect().toList
    assertEquals(variants, List(variant))

    def overlaps = spark.read.json(configuration.output + "/lut/overlap-index/").as[LocusOverlap].collect().toList
    assertEquals(overlaps, List(overlap))
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
