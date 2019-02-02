package ot.geckopipe

/**
  * Case class to map to a gtex section in the configuration file.
  *
  * @param tissueMap a file to the tsv file containing "tissuename\tcode"
  * @param variantGenePairs a file pattern to the tsv file containing significative variant gene pairs
  */
case class GTExSection(tissueMap: String, variantGenePairs: String)

case class EnsemblSection(lut: String, geneTranscriptPairs: String)

case class VEPSection(homoSapiensCons: String, homoSapiensConsScores: String)

case class IntervalSection(path: String)

case class QTLSection(path: String)

case class NearestSection(tssDistance: Long, path: String)

case class VariantSection(raw: String, path: String, nearestGenes: String)

case class VariantGeneSection(path: String)

case class VariantDiseaseSection(path: String, studies: String, toploci: String,
                                 finemapping: String, ld: String, overlapping: String)

/** Main configuration case class */
case class Configuration(output: String,
                         sampleFactor: Double,
                         sparkUri: String,
                         logLevel: String,
                         ensembl: EnsemblSection,
                         vep: VEPSection,
                         interval: IntervalSection,
                         qtl: QTLSection,
                         nearest: NearestSection,
                         variantIndex: VariantSection,
                         variantGene: VariantGeneSection,
                         variantDisease: VariantDiseaseSection)

object Configuration {
  // companion object but nothing at the moment
}
