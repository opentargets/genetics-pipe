package ot.geckopipe.domain

case class V2G(
    chr_id: String,
    position: Long,
    ref_allele: String,
    alt_allele: String,
    gene_id: String,
    feature: String,
    type_id: String,
    source_id: String,
    interval_score: Option[Double] = None,
    interval_score_q: Option[Double] = None,
    d: Option[Long] = None,
    distance_score: Option[Double] = None,
    distance_score_q: Option[Double] = None,
    qtl_beta: Option[Double] = None,
    qtl_se: Option[Double] = None,
    qtl_pval: Option[Double] = None,
    qtl_score: Option[Double] = None,
    qtl_score_q: Option[Double] = None,
    fpred_labels: List[String] = List(),
    fpred_scores: List[Double] = List(),
    fpred_max_label: Option[String] = None,
    fpred_max_score: Option[Double] = None
)
