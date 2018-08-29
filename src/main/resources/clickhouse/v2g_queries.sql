-- break down when chr_id is not equal to gene_id
select feature, chr_id, gene_chr, uniq(variant_id) as uniq_variants, uniq(gene_id) as uniq_genes, count(feature) as num_features
from ot.v2g
where chr_id != gene_chr
group by feature, chr_id, gene_chr
order by num_features desc;

-- get info
select gene_id, type_id, source_id, feature,
  any(variant_id) as variant_it,
  max(csq_counts) as max_csq_counts,
  min(qtl_beta) as max_qtl_beta,
  min(qtl_se) as max_qtl_se,
  min(qtl_pval) as max_qtl_pval,
  max(interval_score) as max_interval_score,
  uniq(variant_id) as uniq_variants
from ot.v2g
where chr_id = '16' and
  position >= 52767042 and
  position <= 54767042
group by gene_id, type_id, source_id, feature
order by uniq_variants desc
limit 10 by type_id, source_id, feature
limit 5000

SELECT
    chr_id,
    gene_id,
    any(gene_start),
    any(gene_end),
    uniq(variant_id),
    count()
FROM ot.v2g
WHERE (chr_id = dictGetString('genes', 'gene_chr', tuple('ENSG00000183117')))
    AND (gene_id = 'ENSG00000183117')
    AND (position >= (dictGetUInt32('genes', 'gene_start', tuple('ENSG00000183117')) - 1000000))
    AND (position <= (dictGetUInt32('genes', 'gene_start', tuple('ENSG00000183117')) + 1000000))
GROUP BY
    chr_id,
    gene_id
LIMIT 20

-- query for compute score
SELECT
    variant_id,
    gene_id,
    avg(source_score) AS total_score
FROM
(
    SELECT
        variant_id,
        gene_id,
        source_id,
        max(ifNull(qtl_score_q, 0.)) AS max_qtl,
        max(ifNull(interval_score_q, 0.)) AS max_int,
        max(ifNull(fpred_max_score, 0.)) AS max_fpred,
        (max_qtl + max_int) + max_fpred AS source_score
    FROM ot.v2g
    PREWHERE (chr_id = '1') AND ((position = 1564952) OR (position = 121346913)) AND (variant_id IN ('1_1564952_TG_T', '1_121346913_A_T'))
    GROUP BY
        variant_id,
        gene_id,
        source_id
)
GROUP BY
    variant_id,
    gene_id
ORDER BY
    variant_id ASC,
    total_score DESC
LIMIT 5 BY variant_id



-- get all variants in a range where they have tissues and
-- and get pval < 1e-6
-- for all the variants also want other consequences
-- based on this input chr 7 pos > 91604921 and pos < 93836594
-- get tissues and count grouped?
-- and get pval < 1e-6
-- TODO finish this example

-- group by feature in a specific region
select type_id, source_id, feature , uniq(gene_id) as unique_genes, uniq(variant_id) as unique_variants, count() as total_evs
from ot.v2g
where chr_id = '7' and position >= 91604921 and position <= 93836594
group by type_id, source_id, feature
order by type_id asc, source_id asc, feature asc;

-- few queries you might find useful for v2g table
-- fixed one gene, give all variants (chr, pos) that are in one tissue but not in others (xor)
select gene_id, variant_id, any(tissue_id) as tid, count(tissue_id) as n_tissues, count() as n_variants
from ot.gtex
where gene_id = 'ENSG00000140718.14' and n_tissues = 1
group by gene_id
limit 10 by gene_id
limit 10;


-- fixed one gene, give lowest pvalue, variant per tissue
select tissue_id, any(variant_id) as lowest_variant_id, min(pval_nominal) as pval_min
from ot.gtex
where gene_id = 'ENSG00000140718.14'
group by tissue_id
order by pval_min asc
limit 10 by tissue_id
limit 10 ;

-- much faster if the partition by chr_id is used as the data is chr partitioned
select tissue_id, any(variant_id) as lowest_variant_id, min(pval_nominal) as pval_min
from ot.gtex
where gene_id = 'ENSG00000140718.14' and chr_id = '16'
group by tissue_id
order by pval_min asc
limit 10 by tissue_id
limit 10 ;
-- get min pval grouping from chr_id and gene_id
select
        chr_id,
        gene_id,
        min(pval_nominal) as min_pval
from ot.gtex
group by chr_id, gene_id
order by min_pval asc
limit 25;

-- get top variants small than a pvalue
CREATE VIEW ot.top_variants AS
SELECT
    chr_id,
    trait_id,
    variant_id,
    any(pos),
    any(pval)
FROM ot.ukbb
WHERE trait_id = 9
GROUP BY
    chr_id,
    trait_id,
    variant_id,
    pval
HAVING pval < 5e-8
ORDER BY pval ASC
