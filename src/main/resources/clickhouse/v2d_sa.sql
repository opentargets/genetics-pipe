create database if not exists ot;
create table if not exists ot.v2d_sa_log(
  rs_id String,
  variant_id String,
  chr_id String,
  position UInt32,
  segment UInt32 MATERIALIZED (position % 1000000),
  ref_allele String,
  alt_allele String,
  eaf Float64,
  beta Float64,
  se Float64,
  pval Float64,
  n UInt32,
  n_cases UInt32,
  stid String)
engine = Log;

-- v2d summary stats partitioned by stid as phewas plot needs all data
create table if not exists ot.v2d_sa_by_stid
engine MergeTree partition by (stid) order by (chr_id, position)
as select
  rs_id,
  variant_id,
  chr_id,
  position,
  segment,
  ref_allele,
  alt_allele,
  eaf,
  beta,
  se,
  pval,
  n,
  n_cases,
  stid
from ot.v2d_sa_log;

-- v2d summary stats partitioned by ch position as regional plot needs all data from summary stats
create table if not exists ot.v2d_sa
engine MergeTree partition by (chr_id) order by (chr_id, position)
as select
  rs_id,
  variant_id,
  chr_id,
  position,
  segment,
  ref_allele,
  alt_allele,
  eaf,
  beta,
  se,
  pval,
  n,
  n_cases,
  stid
from ot.v2d_sa_log;