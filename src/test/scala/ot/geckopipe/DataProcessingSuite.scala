package ot.geckopipe

import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.{Encoders, SparkSession}
import ot.geckopipe.domain._

object DataProcessingSuite extends LocalSparkSessionSuite("spark-tests") {

  val error = 1E-3

  testWithSpark("calculate variant index") { ss =>
    val configuration = createTestConfiguration()
    createRawVariantIndexParquet(configuration.variantIndex.raw)(ss)
    createEnsemblLutJson(configuration.ensembl.lut)(ss)

    Main.run(CommandLineArgs(command = Some("variant-index")), configuration)(ss)

    import ss.implicits._
    val variants = ss.read
      .schema(Encoders.product[Variant2].schema)
      .parquet(configuration.variantIndex.path)
      .as[Variant2]
      .collect()
      .toList

    assertEquals(variants, List(variant2))
  }

  testWithSpark("calculate distance nearest") { ss =>
    val configuration = createTestConfiguration()
    createVariantIndexParquet(configuration.variantIndex.path)(ss)
    createEnsemblLutJson(configuration.ensembl.lut)(ss)

    Main.run(CommandLineArgs(command = Some("distance-nearest")), configuration)(ss)

    import ss.implicits._
    val nearest = ss.read
      .schema(Encoders.product[Nearest].schema)
      .json(configuration.nearest.path)
      .as[Nearest]
      .collect
      .toList

    assertEquals(nearest.length, 1)
    val firstNearest = nearest.head
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

  testWithSpark("calculate variant to gene") { ss =>
    val configuration = createTestConfiguration()
    createRawVariantIndexParquet(configuration.variantIndex.raw)(ss) //TODO Why raw variant index involved if there is variant index already?
    createVariantIndexParquet(configuration.variantIndex.path)(ss)
    createVepConsequenceJson(configuration.vep.homoSapiensConsScores)(ss)
    createEnsemblLutJson(configuration.ensembl.lut)(ss)
    createQtlParquet(configuration.qtl.path)(ss)
    createIntervalParquet(configuration.interval.path)(ss)

    Main.run(CommandLineArgs(command = Some("variant-gene")), configuration)(ss)

    import ss.implicits._
    val v2gs = ss.read
      .schema(Encoders.product[V2G].schema)
      .json(configuration.variantGene.path)
      .as[V2G]
      .collect()

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

  testWithSpark("calculate variant to disease") { ss =>
    val configuration = createTestConfiguration()
    createVariantIndexParquet(configuration.variantIndex.path)(ss)
    createStudyParquet(configuration.variantDisease.studies)(ss)
    createTopLociParquet(configuration.variantDisease.toploci)(ss)
    createLdParquet(configuration.variantDisease.ld)(ss)
    createFineMappingParquet(configuration.variantDisease.finemapping)(ss)

    Main.run(CommandLineArgs(command = Some("variant-disease")), configuration)(ss)

    import ss.implicits._
    val v2d = ss.read
      .schema(Encoders.product[V2D].schema)
      .json(configuration.variantDisease.path)
      .as[V2D]
      .head()

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

  testWithSpark("calculate disease to variant to gene") { ss =>
    val configuration = createTestConfiguration()
    createV2GJson(configuration.variantGene.path)(ss)
    createV2DJson(configuration.variantDisease.path)(ss)

    Main.run(CommandLineArgs(command = Some("disease-variant-gene")), configuration)(ss)

    import ss.implicits._
    val d2v2gs = ss.read
      .schema(Encoders.product[D2V2G].schema)
      .json(configuration.output + "/d2v2g/")
      .as[D2V2G]
      .collect()

    assertEquals(d2v2gs.length, 1)
  }

  testWithSpark("calculate dictionaries") { ss =>
    val configuration = createTestConfiguration()
    createVariantIndexParquet(configuration.variantIndex.path)(ss)
    createStudyParquet(configuration.variantDisease.studies)(ss)
    createOverlapParquet(configuration.variantDisease.overlapping)(ss)
    createEnsemblLutJson(configuration.ensembl.lut)(ss)

    Main.run(CommandLineArgs(command = Some("dictionaries")), configuration)(ss)

    import ss.implicits._

    val genes = ss.read
      .schema(Encoders.product[Gene].schema)
      .json(configuration.output + "/lut/genes-index/")
      .as[Gene]
      .collect()
      .toList

    assertEquals(genes, List(gene))

    val studies = ss.read
      .schema(Encoders.product[Study].schema)
      .json(configuration.output + "/lut/study-index/")
      .as[Study]
      .collect()
      .toList

    assertEquals(studies, List(study))

    val variants = ss.read
      .schema(Encoders.product[Variant3].schema)
      .json(configuration.output + "/lut/variant-index/")
      .as[Variant3]
      .collect()
      .toList

    assertEquals(variants, List(variant3))

    val overlaps = ss.read
      .schema(Encoders.product[LocusOverlap].schema)
      .json(configuration.output + "/lut/overlap-index/")
      .as[LocusOverlap]
      .collect()
      .toList

    assertEquals(overlaps, List(overlap))
  }

  testWithSpark("calculate variant to disease colocation") { ss =>
    val configuration = createTestConfiguration()
    createVariantIndexParquet(configuration.variantIndex.path)(ss)
    createEnsemblLutJson(configuration.ensembl.lut)(ss)
    createV2DColocParquet(configuration.variantDisease.coloc)(ss)

    Main.run(CommandLineArgs(command = Some("variant-disease-coloc")), configuration)(ss)

    import ss.implicits._

    val v2dcolocs = ss.read
      .schema(Encoders.product[V2DColoc].schema)
      .json(configuration.output + "/v2d_coloc/")
      .as[V2DColoc]
      .collect()
      .toList

    assertEquals(v2dcolocs.length, 1)

    val v2dc = v2dcolocs.head
    assertEquals(v2dc, v2dColoc)
  }

  private def createTestConfiguration(): Configuration = {
    val uuid = UUID.randomUUID().toString

    val testDataFolder = Files.createTempDirectory(uuid).toAbsolutePath.toString
    println(s"Writing content to $testDataFolder")
    val inputFolder = s"$testDataFolder/input"
    val outputFolder = s"$testDataFolder/output"

    val configuration = Configuration(
      output = outputFolder,
      sampleFactor = 0, //disabled
      sparkUri = "", //empty string for local
      logLevel = "INFO",
      ensembl = EnsemblSection(lut = s"$inputFolder/hg38.json"),
      vep = VEPSection(homoSapiensConsScores = s"$inputFolder/vep_consequences.tsv"),
      interval = IntervalSection(path = s"$inputFolder/v2g/interval/*/*/data.parquet/"),
      qtl = QTLSection(path = s"$inputFolder/v2g/qtl/*/*/data.parquet/"),
      nearest =
        NearestSection(tssDistance = 500000, path = s"$outputFolder/distance/canonical_tss/"),
      variantIndex = VariantSection(raw = s"$inputFolder/variant-annotation.parquet/",
                                    path = s"$outputFolder/variant-index/",
                                    tssDistance = 500000),
      variantGene = VariantGeneSection(path = s"$outputFolder/v2g/"),
      variantDisease = VariantDiseaseSection(
        path = s"$outputFolder/v2d/",
        studies = s"$inputFolder/v2d/studies.parquet",
        toploci = s"$inputFolder/v2d/toploci.parquet",
        finemapping = s"$inputFolder/v2d/finemapping.parquet",
        ld = s"$inputFolder/v2d/ld.parquet",
        overlapping = s"$inputFolder/v2d/locus_overlap.parquet",
        coloc = s"$inputFolder/coloc/010101/"
      )
    )

    configuration
  }

  private var variant1 = Variant1(
    "1",
    1000,
    "1",
    1100,
    "A",
    "T",
    "rs123",
    Vep(
      most_severe_consequence = "severe consequence",
      transcript_consequences = Array(
        TranscriptConsequence(gene_id = "gene id", consequence_terms = Array("consequence term 1")))
    ),
    Cadd(0.1, 0.2),
    Gnomad(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
  )

  private def createRawVariantIndexParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    List(variant1).toDS.write.parquet(path)
  }

  private val variant2 = Variant2(
    "1",
    1100L,
    "A",
    "T",
    Some("rs123"),
    Some("severe consequence"),
    Cadd(0.1, 0.2),
    Gnomad(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
    Some(10769L),
    Some("ENSG00000223972"),
    Some(10769L),
    Some("ENSG00000223972")
  )

  private def createVariantIndexParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    List(variant2).toDS.write.parquet(path)
  }

  private val variant3 = Variant3(
    "1",
    1100L,
    "A",
    "T",
    Some("rs123"),
    Some("severe consequence"),
    Some(0.1),
    Some(0.2),
    Some(0.1),
    Some(0.2),
    Some(0.3),
    Some(0.4),
    Some(0.5),
    Some(0.6),
    Some(0.7),
    Some(0.8),
    Some(0.9),
    Some(1.0),
    Some(10769L),
    Some("ENSG00000223972"),
    Some(10769L),
    Some("ENSG00000223972")
  )

  private val gene = Gene(
    "1",
    "ENSG00000223972",
    "DDX11L1",
    "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1 [Source:HGNC Symbol;Acc:37102]",
    fwdstrand = true,
    exons = Seq(11869L, 12227L, 12613L, 12721L, 13221L, 14409L),
    11869,
    11869,
    14412,
    "protein_coding"
  )

  private def createEnsemblLutJson(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(gene).toDF().write.json(path)
  }

  private def createVepConsequenceJson(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

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
    ).toDF().write.json(path)
  }

  private def createQtlParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      Qtl("1", 1100L, "A", "T", "ENSG00000223972", 0.1, 0.2, 0.3, "type 1", "src 1", "feature 1")
    ).toDF().write.parquet(path)
  }

  private def createIntervalParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      IntervalDomain("1", 900L, 1200L, "ENSG00000223972", 0.1, "cell type 1", "feature 1")
    ).toDF().write.parquet(path)
  }

  private val study = Study(
    "study1",
    "trait reported 1",
    List("EFO_test"),
    Some("PMID:1"),
    Some("2012-01-03"),
    Some("journal 1"),
    Some("pub title 1"),
    Some("Pub Author"),
    List("European=10"),
    List("European=5"),
    Some(10L),
    Some(5L),
    Some(1L),
    Some("trait category 1"),
    Some(2L)
  )

  private def createStudyParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(study).toDF().write.parquet(path)
  }

  private val topLoci = TopLoci("study1",
                                "1",
                                1100L,
                                "A",
                                "T",
                                Some("+"),
                                Some(0.026),
                                Some(0.021),
                                Some(0.030),
                                Some(0.11),
                                Some(0.09),
                                Some(0.12),
                                Some(2.3),
                                Some(-16))

  private def createTopLociParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(topLoci).toDF().write.parquet(path)
  }

  private def createLdParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      Ld("study1",
         "1",
         1100L,
         "A",
         "T",
         "1",
         1100L,
         "A",
         "T",
         Some(0.9),
         Some(0.1),
         Some(0.2),
         Some(0.3),
         Some(0.4),
         Some(0.5),
         Some(true))
    ).toDF().write.parquet(path)
  }

  private def createFineMappingParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      FineMapping("study1", "1", 1100L, "A", "T", "1", 1100L, "A", "T", Some(28.9), Some(0.021))
    ).toDF().write.parquet(path)
  }

  private val overlap =
    LocusOverlap("study1", "1", 1100L, "A", "T", "study2", "1", 1100L, "A", "T", 7, 3, 5)

  private def createOverlapParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(overlap).toDF().write.parquet(path)
  }

  private val v2g = V2G(
    chr_id = "1",
    position = 1100L,
    ref_allele = "A",
    alt_allele = "T",
    gene_id = "ENSG00000223972",
    type_id = "distance",
    feature = "unspecified",
    source_id = "canonical_tss",
    d = Some(10769L),
    distance_score = Some(9.2),
    distance_score_q = Some(0.1)
  )

  private def createV2GJson(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(v2g).toDF().write.json(path)
  }

  private val v2d = V2D(
    study_id = "study1",
    pmid = Some("PMID:1"),
    pub_date = Some("2012-01-03"),
    pub_journal = Some("journal 1"),
    pub_author = Some("Pub Author"),
    trait_reported = "trait reported 1",
    trait_efos = List("EFO_test"),
    ancestry_initial = List("European=10"),
    ancestry_replication = List("European=5"),
    n_initial = Some(10),
    n_replication = Some(5),
    n_cases = Some(1),
    trait_category = Some("trait category 1"),
    num_assoc_loci = Some(2),
    lead_chrom = "1",
    lead_pos = 1100L,
    lead_ref = "A",
    lead_alt = "T",
    tag_chrom = "1",
    tag_pos = 1100L,
    tag_ref = "A",
    tag_alt = "T",
    overall_r2 = Some(0.9),
    posterior_prob = Some(0.021),
    pval = 1.1,
    pval_mantissa = 2.3,
    pval_exponent = -16
  )

  private def createV2DJson(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(v2d).toDF().write.json(path)
  }

  private val v2dColoc = V2DColoc(
    left_study = "study2",
    left_chrom = "1",
    left_pos = 1100L,
    left_ref = "A",
    left_alt = "T",
    left_type = "gwas",
    right_study = "study1",
    right_chrom = "1",
    right_pos = 1100L,
    right_ref = "A",
    right_alt = "T",
    right_type = "gwas",
    right_gene_id = "ENSG00000223972",
    coloc_h3 = Some(0.1)
  )

  private def createV2DColocParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(v2dColoc).toDF().write.parquet(path)
  }
}
