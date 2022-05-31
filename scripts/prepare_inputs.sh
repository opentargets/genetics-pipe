#!/bin/bash

set -x

echo "Preparing to copy genetics resources"
staging='gs://genetics-portal-dev-staging'
dev_data='gs://genetics-portal-dev-data'
release='22.04'
previous_inputs='gs://genetics-portal-dev-data/21.10/inputs'
sumstats='gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005'

# Some files are tagged with a date and we need to select the correct one. Right now we have to look at the available
# files in the staging bucket and select the best one.
# find with gsutil ls gs://genetics-portal-dev-staging/lut/biofeature_labels/
b_lut='220212'
#trait_efo='2021-02-14'
# find with gsutil ls gs://genetics-portal-dev-staging/v2d/
v2d_version='220401'
# find with gsutil ls gs://genetics-portal-dev-staging/coloc/
coloc='220331'
# find with gsutil ls gs://genetics-portal-dev-staging/v2g/qtl/
qtl='220331'
# listed under gs://genetics-portal-dev-staging/finemapping/
finemapping='220224_merged'
# listed under gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005/gwas
gwas='220330'
# listed under gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005/molecular_trait
molecular_trait='220330'

# -n flag so as not to clobber existing.
gscp='gsutil -m cp -n'
inputs=$dev_data/$release/inputs
outputs=$dev_data/$release/outputs
lut=$inputs/lut
v2d=$inputs/v2d
v2g=$inputs/v2g
va=$inputs/variant-annotation
sa=$outputs/sa
sagwas=$sa/gwas
samt=$sa/molecular_trait

echo "copy static files from previous release"
# copy static files from previous release
#lut
lut_files=('biofeature_labels.json' 'vep_consequences.tsv' )
for i in "${lut_files[@]}"
do
  echo "Copy $previous_inputs/lut/$i to $lut"
  $gscp $previous_inputs/lut/$i $lut/$i
done

echo "Add date versioned LUT inputs"
# add date versioned lut inputs
echo "Copy $staging/lut/biofeature_labels/$b_lut/biofeature_labels.w_composites.json to $lut/biofeature_lut_$b_lut.w_composites.json"
$gscp $staging/lut/biofeature_labels/$b_lut/biofeature_labels.w_composites.json \
"$lut/biofeature_lut_$b_lut.w_composites.json"

echo "Add versioned v2d inputs"
v2d_files=( 'studies.parquet' \
            'ld_analysis_input.tsv' \
            'locus_overlap.parquet' \
            'trait_efo.parquet' \
            'finemapping.parquet' \
            'toploci.parquet' \
            'ld.parquet')
for i in "${v2d_files[@]}"
do
	echo "Copy $staging/v2d/$v2d_version/$i to $v2d/$i"
    $gscp -r $staging/v2d/$v2d_version/$i $v2d/$i
done

echo "Add colocation files to v2d inputs"
colocIn=$staging/coloc/$coloc/coloc_processed_w_betas_fixed.parquet
echo "Copy $colocIn to $v2d"
$gscp -r $colocIn $v2d


echo "Add versioned v2g inputs"
echo "Copy $staging/v2g/interval to $v2g/interval"
$gscp -r $staging/v2g/interval $v2g/interval
echo "Copy $staging/v2g/qtl/$qtl $v2g/qtl/"
$gscp -r $staging/v2g/qtl/$qtl $v2g/qtl/

echo "Add variant annotations from previous release"
# In theory these files could be updated in future, but we're still using the Jan 2019 ones
# so they are effectively static.
echo "copy $previous_inputs/variant-annotation/ to $va"
$gscp -r $previous_inputs/variant-annotation/ $va


echo "COPY STATIC INPUTS: SUMSTATS AND CREDSET"

echo "Copy sumstats -- not used in pipeline, only for clickhouse"
echo "Copy $sumstats/gwas/$gwas to $sagwas"
$gscp -r $sumstats/gwas/$gwas $sagwas
echo "Copy $sumstats/molecular_trait/$molecular_trait to $samt"
$gscp -r $sumstats/molecular_trait/$molecular_trait $samt

echo "Copy credset -- not used in pipeline"
echo "Copy $staging/finemapping/$finemapping/credset/* to $outputs/v2d_credset/"
$gscp -r $staging/finemapping/$finemapping/credset/* $outputs/v2d_credset/

echo "Done downloading"

echo $"gsutil ls $inputs"
