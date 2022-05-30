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

case class ScoredDatasetsSection(variantGeneByOverall: String,
                                 variantGeneScored: String,
                                 diseaseVariantGeneByOverall: String,
                                 diseaseVariantGeneScored: String)

/**
  * Leaving this as snake case because it's converted to a data-frame elsewhere in the code.
  */
case class SourceIdAndWeight(source_id: String, weight: Double)
case class VariantGeneSection(path: String, weights: Seq[SourceIdAndWeight])
case class DiseaseVariantGeneSection(path: String)

case class VariantDiseaseSection(path: String,
                                 studies: String,
                                 toploci: String,
                                 finemapping: String,
                                 ld: String,
                                 overlapping: String,
                                 coloc: String,
                                 efos: String)

case class ManhattanSection(locusGene: String,
                            diseaseVariantGeneScored: String,
                            variantDiseaseColoc: String,
                            variantDisease: String,
                            path: String)

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
                         scoredDatasets: ScoredDatasetsSection,
                         manhattan: ManhattanSection)

object Configuration {
  // companion object but nothing at the moment
}
