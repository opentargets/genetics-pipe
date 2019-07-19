package ot.geckopipe

import java.util.UUID

import org.apache.spark.sql.SparkSession
import ot.geckopipe.domain._

object DataProcessingSuite extends LocalSparkSessionSuite("spark-tests") {

  test("calculate variant index") {
    withSpark { ss =>
      val configuration = createTestConfiguration()
      createRawVariantIndexParquet(configuration.variantIndex.raw)(ss)
      createEnsemblLutJson(configuration.ensembl.lut)(ss)

      Main.run(CommandLineArgs(command = Some("variant-index")), configuration)(ss)

      import ss.implicits._
      val variants = ss.read.parquet(configuration.variantIndex.path).as[Variant].collect().toList

      assertEquals(variants, List(
        Variant("1", 1100, "1", 1000, "A", "T", "rs123", "severe consequence", "cadd 1", "af 1", 10769L, "ENSG00000223972",
          10769L, "ENSG00000223972")
      ))
    }
  }

//  test("calculate distance nearest") {
//    withSpark { ss =>
//        Main.run(CommandLineArgs(command = Some("distance-nearest")), configuration)(ss)
//
//        val firstNearest = ss.read.json(configuration.nearest.path).as[Nearest].collect.toList.headOption
//        assertEquals(firstNearest.isDefined, true)
//
//        // there is one so apply when there is one
//        firstNearest.foreach {
//          case nearest =>
//            assertEquals(nearest.chr_id, "1")
//            assertEquals(nearest.position, 1100L)
//            assertEquals(nearest.ref_allele, "A")
//            assertEquals(nearest.alt_allele, "T")
//            assertEquals(nearest.gene_id, "ENSG00000223972")
//            assertEquals(nearest.d, 10769L)
//            val error = 0.001
//            assert(nearest.distance_score - 9.285 < error)
//            assert(nearest.distance_score_q - 0.1 < error)
//            assertEquals(nearest.type_id, "distance")
//            assertEquals(nearest.source_id, "canonical_tss")
//            assertEquals(nearest.feature, "unspecified")
//        }
//    }
//  }

  private def createTestConfiguration(): Configuration = {
    val uuid = UUID.randomUUID().toString

    val testDataFolder = s"/tmp/tests-$uuid"
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



    configuration
  }

  private def createRawVariantIndexParquet(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      RawVariant("1", 1000, "1", 1100, "A", "T", "rs123", Vep(most_severe_consequence = "severe consequence"), "cadd 1", "af 1")
    ).toDF().write.parquet(path)
  }

  private def createEnsemblLutJson(path: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._

    Seq(
      Gene("1", "ENSG00000223972", 11869, 11869, 14412, "protein_coding")
    ).toDF().write.json(path)
  }

}
