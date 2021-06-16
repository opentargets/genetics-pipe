package ot.geckopipe.domain

case class Study(study_id: String,
                 trait_reported: String,
                 source: Option[String],
                 trait_efos: List[String],
                 pmid: Option[String] = None,
                 pub_date: Option[String] = None,
                 pub_journal: Option[String] = None,
                 pub_title: Option[String] = None,
                 pub_author: Option[String] = None,
                 ancestry_initial: List[String],
                 ancestry_replication: List[String],
                 n_initial: Option[Long] = None,
                 n_replication: Option[Long] = None,
                 n_cases: Option[Long] = None,
                 trait_category: Option[String] = None,
                 num_assoc_loci: Option[Long] = None)
