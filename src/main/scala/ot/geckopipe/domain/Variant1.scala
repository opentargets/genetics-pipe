package ot.geckopipe.domain

case class TranscriptConsequence(gene_id: String, consequence_terms: Array[String])

case class Vep(
    most_severe_consequence: String,
    transcript_consequences: Array[TranscriptConsequence]
)

case class Variant1(
    chrom_b37: String,
    pos_b37: Long,
    chrom_b38: String,
    pos_b38: Long,
    ref: String,
    alt: String,
    rsId: String,
    vep: Vep,
    cadd: Cadd,
    af: Gnomad
)
