-- a good idea is to include the source_name in order to partition by (chr_id and source_name)

-- variantindex ("chr_id", "position", "ref_allele", "alt_allele", "variant_id", "rs_id")
-- geneindex ("gene_chr", "gene_id", "gene_start", "gene_end", "gene_name")
-- v2g additionals ("feature", "value") + variantindex + geneindex
-- drop and create the table in the case it exists
create database if not exists ot;
create table if not exists ot.v2g_log(
  chr_id String,
  position UInt32,
  ref_allele String,
  alt_allele String,
  variant_id String,
  rs_id String,
  gene_chr String,
  gene_id String,
  gene_start UInt32,
  gene_end UInt32,
  gene_name String,
  type_id String,
  source_id String,
  feature String,
  value Array(Float64))
engine = Log;

-- how insert the data from files into the log db
insert into ot.v2g_log format JSONEachRow from '/opt/out/v2g/*.json';

-- main v2g table with proper mergetree engine
-- maybe partition by chr_id and source_id
create table if not exists ot.v2g
engine MergeTree partition by (chr_id) order by (chr_id, position)
as select
  chr_id,
  position,
  ref_allele,
  alt_allele,
  variant_id,
  rs_id,
  gene_chr,
  gene_id,
  gene_start,
  gene_end,
  gene_name,
  source_id,
  type_id,
  feature,
  value
from ot.v2g_log;
