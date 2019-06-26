package ot.geckopipe.domain

case class Gene(chr: String,
                gene_id: String,
                tss: Long,
                start: Long,
                end: Long,
                biotype: String)
