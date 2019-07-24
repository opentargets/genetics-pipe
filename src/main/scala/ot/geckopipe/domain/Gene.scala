package ot.geckopipe.domain

case class Gene(chr: String,
                gene_id: String,
                gene_name: String,
                description: String,
                fwdstrand: Boolean,
                exons: Seq[Long],
                tss: Long,
                start: Long,
                end: Long,
                biotype: String)
