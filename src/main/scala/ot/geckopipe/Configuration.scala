package ot.geckopipe

/**
  * Case class to map to a gtex section in the configuration file.
  *
  * @param tissueMap a file to the tsv file containing "tissuename\tcode"
  * @param variantGenePairs a file pattern to the tsv file containing significative variant gene pairs
  */
case class GTExSection(tissueMap: String, variantGenePairs: String)

case class EnsemblSection(lut: String)

case class VEPSection(homoSapiensConsScores: String)

case class IntervalSection(path: String)

case class QTLSection(path: String)

case class NearestSection(tssDistance: Long, path: String)

case class VariantSection(raw: String, path: String, tssDistance: Long)

case class ScoredDatasetsSection(v2gByOverall: String, d2v2gByOverall: String, d2v2gScored: String)

case class VariantGeneSection(path: String, weights: String)
case class DiseaseVariantGeneSection(path: String)

case class VariantDiseaseSection(path: String,
                                 studies: String,
                                 toploci: String,
                                 finemapping: String,
                                 ld: String,
                                 overlapping: String,
                                 coloc: String,
                                 efos: String)

/** Main configuration case class */
case class Configuration(output: String,
                         format: String,
                         sampleFactor: Double,
                         sparkUri: Option[String],
                         logLevel: String,
                         ensembl: EnsemblSection,
                         vep: VEPSection,
                         interval: IntervalSection,
                         qtl: QTLSection,
                         nearest: NearestSection,
                         variantIndex: VariantSection,
                         variantGene: VariantGeneSection,
                         variantDisease: VariantDiseaseSection,
                         diseaseVariantGene: DiseaseVariantGeneSection,
                         scoredDatasets: ScoredDatasetsSection)

object Configuration {
  // companion object but nothing at the moment
}
