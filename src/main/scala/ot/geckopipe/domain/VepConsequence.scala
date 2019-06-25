package ot.geckopipe.domain

case class VepConsequence(accession: String,
                          term: String,
                          description: String,
                          display_term: String,
                          impact: String,
                          v2g_score: Double,
                          eco_score: Double)
