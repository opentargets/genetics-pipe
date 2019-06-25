package ot.geckopipe.domain

case class Gene(chr: String,
                gene_id: String,
                tss: Int,
                start: Int,
                end: Int,
                biotype: String)
