package ot.geckopipe.domain

case class Study(study_id: String,
                 pmid: Option[String] = None,
                 pub_date: Option[String] = None,
                 pub_journal: Option[String] = None,
                 pub_title: Option[String] = None,
                 pub_author: Option[String] = None,
                 trait_reported: Option[String] = None,
                 trait_efos: Option[List[String]] = None,
                 ancestry_initial: Option[List[String]] = None,
                 ancestry_replication: Option[List[String]] = None,
                 n_initial: Option[Long] = None,
                 n_replication: Option[Long] = None,
                 n_cases: Option[Long] = None,
                 trait_category: Option[String] = None,
                 num_assoc_loci: Option[Long] = None,
                 has_sumstats: Option[Boolean] = None)
