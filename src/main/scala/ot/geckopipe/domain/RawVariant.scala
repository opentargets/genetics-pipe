package ot.geckopipe.domain

case class Vep(most_severe_consequence: String)

case class RawVariant(chrom_b37: String,
                      pos_b37: Int,
                      chrom_b38: String,
                      pos_b38: Int,
                      ref: String,
                      alt: String,
                      rsId: String,
                      vep: Vep,
                      cadd: String,
                      af: String)
