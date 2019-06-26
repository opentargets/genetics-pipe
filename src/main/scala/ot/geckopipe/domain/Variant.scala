package ot.geckopipe.domain

case class Variant(chr_id: String,
                   position: Long,
                   chr_id_b37: String,
                   position_b37: Long,
                   ref_allele: String,
                   alt_allele: String,
                   rs_id: String,
                   most_severe_consequence: String,
                  /*TODO How to make them nullable? AnalysisException: cannot resolve '`raw`' given input columns:
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
                   */
                   gene_id_any_distance: Long,
                   gene_id_any: String,
                   gene_id_prot_coding_distance: Long,
                   gene_id_prot_coding: String)
