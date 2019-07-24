package ot.geckopipe.domain

case class Qtl(chrom: String,
               pos: Long,
               other_allele: String,
               effect_allele: String,
               ensembl_id: String,
               beta: Double,
               se: Double,
               pval: Double,
              //TODO These fields don't seem to be in the schema
               `type`: String,
               source: String,
               feature: String)
