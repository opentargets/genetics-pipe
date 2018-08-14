-- a good idea is to include the source_name in order to partition by (chr_id and source_name)

-- variantindex ("chr_id", "position", "ref_allele", "alt_allele", "variant_id", "rs_id")
-- geneindex ("gene_chr", "gene_id", "gene_start", "gene_end", "gene_name")
-- v2g additionals ("feature", "value") + variantindex + geneindex
-- drop and create the table in the case it exists
create database if not exists ot;
create table if not exists ot.v2g_log(
  chr_id String,
  position UInt32,
  segment UInt32 MATERIALIZED (position % 1000000),
  ref_allele String,
  alt_allele String,
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
  fpred_labels Array(String),
  fpred_scores Array(Float64),
  qtl_beta Nullable(Float64),
  qtl_se Nullable(Float64),
  qtl_pval Nullable(Float64),
  interval_score Nullable(Float64))
engine = Log;

-- how insert the data from files into the log db
insert into ot.v2g_log format TabSeparatedWithNames from '/opt/out/v2g/*.json';
-- for line in $(cat list_files.txt); do
--  gsutil cat $line | clickhouse-client --query="insert into ot.v2g_log format TabSeparatedWithNames ";
-- done

-- main v2g table with proper mergetree engine
-- maybe partition by chr_id and source_id
create table if not exists ot.v2g
engine MergeTree partition by (chr_id) order by (chr_id, position)
as select
  assumeNotNull(chr_id) as chr_id,
  assumeNotNull(position) as position,
  assumeNotNull(segment) as segment,
  assumeNotNull(ref_allele) as ref_allele,
  assumeNotNull(alt_allele) as alt_allele,
  assumeNotNull(variant_id) as variant_id,
  assumeNotNull(rs_id) as rs_id,
  assumeNotNull(gene_chr) as gene_chr,
  assumeNotNull(gene_id) as gene_id,
  assumeNotNull(gene_start) as gene_start,
  assumeNotNull(gene_end) as gene_end,
  assumeNotNull(gene_type) as gene_type,
  assumeNotNull(gene_name) as gene_name,
  assumeNotNull(feature) as feature,
  assumeNotNull(type_id) as type_id,
  assumeNotNull(source_id) as source_id,
  ifNull(csq_counts, NULL) as csq_counts,
  ifNull(qtl_beta, NULL) as qtl_beta,
  ifNull(qtl_se, NULL) as qtl_se,
  ifNull(qtl_pval, NULL) as qtl_pval,
  ifNull(interval_score, NULL) as interval_score
from ot.v2g_log;

create table if not exists ot.v2g_nested
(
  chr_id String,
  position UInt32,
  segment UInt32,
  ref_allele String,
  alt_allele String,
  variant_id String,
  rs_id String,
  gene_chr String,
  gene_id String,
  gene_start UInt32,
  gene_end UInt32,
  gene_type String,
  gene_name String,
  type_id String,
  source_id String,
  feature String,
  fpred Nested
  (
    label String,
    score Float64
  ),
  beta Float64,
  se Float64,
  pval Float64,
  interval_score Float64
)
engine MergeTree partition by (chr_id) order by (chr_id, position)

insert into ot.v2g_nested VALUES
  ('1', 160650838, 650838, 'T', 'G', '1_160650838_T_G', 'rs1051675500', '1', 'ENSG00000162755' , 161068151, 161070136, 'KLHDC9', ['pchic', 'pchic'], ['javierre2016', 'javierre2016'], ['erythroblasts', 'naive_cd8'], [6.0584891714175795, 7.40737934842716], ['eqtl'], ['gtex_v7'], ['vagina'], [3.34234], [0.23452], [0.00001], ['fpred'], ['vep'], ['unspecified'], ['missense'], ['MODIFIER'], [0.2] ),
  ('1', 160650838, 650838, 'T', 'G', '1_160650838_T_G', 'rs1051675500', '1', 'ENSG00000179914' , 160846329, 160854960, 'ITLN1', ['pchic', 'pchic'], ['javierre2016', 'javierre2016'], ['erythroblasts', 'naive_cd8'], [6.0584891714175795, 7.40737934842716], ['eqtl'], ['gtex_v7'], ['vagina'], [3.34234], [0.23452], [0.00001], ['fpred'], ['vep'], ['unspecified'], ['missense'], ['MODIFIER'], [0.2] ),
  ('1', 160650838, 650838, 'T', 'G', '1_160650838_T_G', 'rs1051675500', '1', 'ENSG00000122223' , 160799950, 160832692, 'CD244', ['pchic', 'pchic'], ['javierre2016', 'javierre2016'], ['erythroblasts', 'naive_cd8'], [6.0584891714175795, 7.40737934842716], ['eqtl'], ['gtex_v7'], ['vagina'], [3.34234], [0.23452], [0.00001], ['fpred'], ['vep'], ['unspecified'], ['missense'], ['MODIFIER'], [0.2] )

insert into ot.v2g_nested
select
  chr_id,
  position,
  segment,
  ref_allele,
  alt_allele,
  variant_id,
  rs_id,
  gene_chr,
  gene_id,
  gene_start,
  gene_end,
  gene_type,
  gene_name,
  type_id,
  source_id,
  feature,
  fpred_labels,
  fpred_scores,
  qtl_beta,
  qtl_se,
  qtl_pval,
  interval_score
from ot.v2g_log;
