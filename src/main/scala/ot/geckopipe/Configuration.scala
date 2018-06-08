package ot.geckopipe

/**
  * Case class to map to a gtex section in the configuration file.
  *
  * @param tissueMap a file to the tsv file containing "tissuename\tcode"
  * @param variantGenePairs a file pattern to the tsv file containing significative variant gene pairs
  */
case class GTExSection(tissueMap: String, variantGenePairs: String)

case class EnsemblSection(geneTranscriptPairs: String)

case class VEPSection(csq: String, homoSapiensCons: String)

case class IntervalSection(pchic: String, dhs: String, fantom5: String)

case class VariantSection(build: Boolean, path: String)
/**
  * Main configuration case class
  *
  * @param sampleFactor enabled if > .0 by default .0
  * @param sparkUri the uri to connect to spark empty by default
  * @param gtex the GTExSection main section
  */
case class Configuration(output: String,
                         sampleFactor: Double,
                         sparkUri: String,
                         logLevel: String,
                         ensembl: EnsemblSection,
                         gtex: GTExSection,
                         vep: VEPSection,
                         interval: IntervalSection,
                         variantIndex: VariantSection)

object Configuration {
  // companion object but nothing at the moment
}
