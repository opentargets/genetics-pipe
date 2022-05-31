package ot.geckopipe.domain

case class TopLoci(
    study_id: String,
    chrom: String,
    pos: Long,
    ref: String,
    alt: String,
    direction: Option[String] = None,
    beta: Option[Double] = None,
    beta_ci_lower: Option[Double] = None,
    beta_ci_upper: Option[Double] = None,
    odds_ratio: Option[Double] = None,
    oddsr_ci_lower: Option[Double] = None,
    oddsr_ci_upper: Option[Double] = None,
    pval_mantissa: Option[Double] = None,
    pval_exponent: Option[Long] = None
)
