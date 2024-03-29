package ot.geckopipe.domain

case class Variant3(
    chr_id: String,
    position: Long,
    ref_allele: String,
    alt_allele: String,
    rs_id: Option[String] = None,
    most_severe_consequence: Option[String] = None,
    raw: Option[Double] = None,
    phred: Option[Double] = None,
    gnomad_afr: Option[Double] = None,
    gnomad_amr: Option[Double] = None,
    gnomad_asj: Option[Double] = None,
    gnomad_eas: Option[Double] = None,
    gnomad_fin: Option[Double] = None,
    gnomad_nfe: Option[Double] = None,
    gnomad_nfe_est: Option[Double] = None,
    gnomad_nfe_nwe: Option[Double] = None,
    gnomad_nfe_onf: Option[Double] = None,
    gnomad_oth: Option[Double] = None,
    gene_id_any_distance: Option[Long] = None,
    gene_id_any: Option[String] = None,
    gene_id_prot_coding_distance: Option[Long] = None,
    gene_id_prot_coding: Option[String] = None
)
