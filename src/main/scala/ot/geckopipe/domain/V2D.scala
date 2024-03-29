package ot.geckopipe.domain

case class V2D(
    study_id: String,
    pmid: Option[String] = None,
    pub_date: Option[String] = None,
    pub_journal: Option[String] = None,
    pub_title: Option[String] = None,
    pub_author: Option[String] = None,
    trait_reported: String,
    trait_efos: List[String],
    ancestry_initial: List[String],
    ancestry_replication: List[String],
    n_initial: Option[Long] = None,
    n_replication: Option[Long] = None,
    n_cases: Option[Long] = None,
    trait_category: Option[String] = None,
    num_assoc_loci: Option[Long] = None,
    lead_chrom: String,
    lead_pos: Long,
    lead_ref: String,
    lead_alt: String,
    tag_chrom: String,
    tag_pos: Long,
    tag_ref: String,
    tag_alt: String,
    overall_r2: Option[Double] = None,
    AFR_1000G_prop: Option[Double] = None,
    AMR_1000G_prop: Option[Double] = None,
    EAS_1000G_prop: Option[Double] = None,
    EUR_1000G_prop: Option[Double] = None,
    SAS_1000G_prop: Option[Double] = None,
    log10_ABF: Option[Double] = None,
    posterior_prob: Option[Double] = None,
    odds_ratio: Option[Double] = None,
    oddsr_ci_lower: Option[Double] = None,
    oddsr_ci_upper: Option[Double] = None,
    direction: Option[String] = None,
    beta: Option[Double] = None,
    beta_ci_lower: Option[Double] = None,
    beta_ci_upper: Option[Double] = None,
    pval_mantissa: Double,
    pval_exponent: Long,
    pval: Double
)
