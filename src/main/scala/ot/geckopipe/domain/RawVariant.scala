package ot.geckopipe.domain

case class TranscriptConsequence(gene_id: String, consequence_terms: Array[String])

case class Vep(most_severe_consequence: String, transcript_consequences: Array[TranscriptConsequence])

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
