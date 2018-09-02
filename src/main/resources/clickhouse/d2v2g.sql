create database if not exists ot;
create table if not exists ot.d2v2g_log(
  chr_id String,
  position UInt32,
  segment UInt32 MATERIALIZED (position % 1000000),
  ref_allele String,
  alt_allele String,
  stid String,
  index_variant_id String,
  r2 Nullable(Float64),
  afr_1000g_prop Nullable(Float64),
  amr_1000g_prop Nullable(Float64),
  eas_1000g_prop Nullable(Float64),
  eur_1000g_prop Nullable(Float64),
  sas_1000g_prop Nullable(Float64),
  log10_abf Nullable(Float64),
  posterior_prob Nullable(Float64),
  pmid Nullable(String),
  pub_date Nullable(String),
  pub_journal Nullable(String),
  pub_title Nullable(String),
  pub_author Nullable(String),
  trait_reported String,
  trait_efos Array(String) default [],
  trait_code String,
  ancestry_initial Nullable(String),
  ancestry_replication Nullable(String),
  n_initial Nullable(UInt32),
  n_replication Nullable(UInt32),
  n_cases Nullable(UInt32),
  pval Float64,
  index_variant_rsid String,
  index_chr_id String,
  index_position UInt32,
  index_ref_allele String,
  index_alt_allele String,
  variant_id String,
  rs_id String,
  gene_chr String,
  gene_id String,
  gene_start UInt32,
  gene_end UInt32,
  gene_type String,
  gene_name String,
  feature String,
  type_id String,
  source_id String,
  fpred_labels Array(String) default [],
  fpred_scores Array(Float64)default [],
  fpred_max_label Nullable(String),
  fpred_max_score Nullable(Float64),
  qtl_beta Nullable(Float64),
  qtl_se Nullable(Float64),
  qtl_pval Nullable(Float64),
  qtl_score Nullable(Float64),
  interval_score Nullable(Float64),
  qtl_score_q Nullable(Float64),
  interval_score_q Nullable(Float64)
)
engine = Log;

create table if not exists ot.d2v2g
engine MergeTree partition by (source_id, chr_id) order by (position)
as select
  chr_id ,
  position,
  segment ,
  ref_allele ,
  alt_allele ,
  stid ,
  index_variant_id ,
  r2,
  afr_1000g_prop ,
  amr_1000g_prop ,
  eas_1000g_prop ,
  eur_1000g_prop ,
  sas_1000g_prop ,
  log10_abf ,
  posterior_prob ,
  pmid ,
  pub_date ,
  pub_journal ,
  pub_title ,
  pub_author ,
  trait_reported ,
  trait_efos ,
  trait_code ,
  ancestry_initial ,
  ancestry_replication ,
  n_initial ,
  n_replication ,
  n_cases ,
  pval ,
  index_variant_rsid ,
  index_chr_id ,
  index_position ,
  index_ref_allele ,
  index_alt_allele ,
  variant_id ,
  rs_id ,
  gene_chr ,
  gene_id ,
  gene_start ,
  gene_end ,
  gene_type ,
  gene_name ,
  feature ,
  type_id ,
  source_id ,
  fpred_labels ,
  fpred_scores ,
  fpred_max_label ,
  fpred_max_score ,
  qtl_beta ,
  qtl_se ,
  qtl_pval ,
  qtl_score ,
  interval_score,
  qtl_score_q ,
  interval_score_q
from ot.d2v2g_log;

create table if not exists ot.d2v2g_score_by_source
engine MergeTree partition by (source_id, chr_id) order by (variant_id, gene_id)
as select
  chr_id,
  variant_id,
  gene_id,
  source_id,
  max(ifNull(qtl_score_q, 0.)) AS max_qtl,
  max(ifNull(interval_score_q, 0.)) AS max_int,
  max(ifNull(fpred_max_score, 0.)) AS max_fpred,
  max_qtl + max_int + max_fpred as source_score
from ot.d2v2g
group by source_id, chr_id, variant_id, gene_id


create table if not exists ot.d2v2g_score_by_overall
engine MergeTree partition by (chr_id) order by (variant_id, gene_id)
as select
  chr_id,
  variant_id,
  gene_id,
  avg(source_score) as overall_score
from ot.d2v2g_score_by_source
group by chr_id, variant_id, gene_id

-- query to join overall scores
-- select gene_id, overall_score from (select variant_id, gene_id from ot.d2v2g prewhere chr_id = '10' and v
-- ariant_id = '10_102075479_G_A' group by variant_id, gene_id) all inner join (select chr_id, variant_id, gene_id, overall_score from ot.d2v2g_score_by_overall p
-- rewhere chr_id = '10' and variant_id = '10_102075479_G_A') using variant_id, gene_id order by overall_score desc
--
-- SELECT
--     gene_id,
--     overall_score
-- FROM
-- (
--     SELECT
--         variant_id,
--         gene_id
--     FROM ot.d2v2g
--     PREWHERE (chr_id = '10') AND (variant_id = '10_102075479_G_A')
--     GROUP BY
--         variant_id,
--         gene_id
-- )
-- ALL INNER JOIN
-- (
--     SELECT
--         chr_id,
--         variant_id,
--         gene_id,
--         overall_score
--     FROM ot.d2v2g_score_by_overall
--     PREWHERE (chr_id = '10') AND (variant_id = '10_102075479_G_A')
-- ) USING (variant_id, gene_id)
-- ORDER BY overall_score DESC

-- join best genes but missing lambda array to get the top ones
-- select index_variant_id, top_genes, len_top_genes from (select index_variant_id from ot.v2d_by_stchr prewhere stid = 'NEALEUKB_50' group by index_variant_id) all inner join (select variant_id as index_variant_id, groupArray(tuple(gene_id,overall_score)) as top_genes, length(top_genes) as len_top_genes from ot.d2v2g_score_by_overall prewhere variant_id = index_variant_id and overall_score >= 0.9 group by variant_id ) using index_variant_id order by len_top_genes desc
--
-- select
--  index_variant_id,
--  top_genes,
--  len_top_genes
-- from
--  (
--    select
--      index_variant_id
--    from ot.v2d_by_stchr
--    prewhere stid = 'NEALEUKB_50'
--    group by index_variant_id
--  )
--    all inner join
--  (
--    select
--      variant_id as index_variant_id,
--      groupArray(tuple(gene_id,overall_score)) as top_genes,
--      length(top_genes) as len_top_genes
--    from ot.d2v2g_score_by_overall
--    prewhere
--      variant_id = index_variant_id and
--      overall_score >= 0.9
--    group by variant_id
--  )
--  using index_variant_id
--  order by len_top_genes desc
