package ot.geckopipe.domain

case class V2DColoc(
    left_study: String,
    left_chrom: String,
    left_pos: Long,
    left_ref: String,
    left_alt: String,
    left_type: String,
    right_study: String,
    right_chrom: String,
    right_pos: Long,
    right_ref: String,
    right_alt: String,
    right_type: String,
    right_gene_id: String,
    coloc_h0: Option[Double] = None,
    coloc_h1: Option[Double] = None,
    coloc_h2: Option[Double] = None,
    coloc_h3: Option[Double] = None,
    coloc_h4: Option[Double] = None,
    coloc_h4_h3: Option[Double] = None,
    coloc_log2_h4_h3: Option[Double] = None
)
