#!/bin/bash

set -x

echo "Preparing to copy genetics resources"
staging='gs://genetics-portal-dev-staging'
dev_data='gs://genetics-portal-dev-data'
release='22.02.1'
previous_inputs='gs://genetics-portal-dev-data/21.10/inputs'
sumstats='gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005'

# Some files are tagged with a date and we need to select the correct one. Right now we have to look at the available
# files in the staging bucket and select the best one.
b_lut='220105'
trait_efo='2021-02-08'
v2d_version='220208'
coloc='220113_merged'
finemapping='210923'
qtl='220105'
# listed under gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005/
gwas='220208'
molecular_trait='220105'

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
lut_files=('biofeature_labels.json' 'vep_consequences.tsv' 'v2g_scoring_source_weights.141021.json')
for i in "${lut_files[@]}"
do
  echo "Copy $i to $lut"
  $gscp $previous_inputs/lut/$i $lut/$i
done

echo "Add date versioned LUT inputs"
# add date versioned lut inputs
$gscp $staging/lut/biofeature_labels/$b_lut/biofeature_labels.w_composites.json \
"$lut/biofeature_lut_$b_lut.w_composites.json"

echo "Add versioned v2d inputs"
v2d_files=( 'studies.parquet' \
            'ld_analysis_input.tsv' \
            'locus_overlap.parquet' \
            $"trait_efo-$trait_efo.parquet" \
            'finemapping.parquet' \
            'toploci.parquet' \
            'ld.parquet')
for i in "${v2d_files[@]}"
do
	echo "Copy $i to $v2d"
    $gscp -r $staging/v2d/$v2d_version/$i $v2d/$i
done

echo "Add colocation files to v2d inputs"
$gscp -r $staging/coloc/$coloc/coloc_processed_w_betas.parquet $v2d


echo "Add versioned v2g inputs"
$gscp -r $staging/v2g/interval $v2g
$gscp -r $staging/v2g/qtl/$qtl $v2g/qtl/

echo "Add variant annotations from previous release"
# In theory these files could be updated in future, but we're still using the Jan 2019 ones
# so they are effectively static.
$gscp -r $previous_inputs/variant-annotation/190129 $va


echo "COPY STATIC INPUTS: SUMSTATS AND CREDSET"

echo "Copy sumstats -- not used in pipeline, only for clickhouse"
$gscp -r $sumstats/gwas/$gwas $sagwas
$gscp -r $sumstats/molecular_trait/$molecular_trait $samt

echo "Copy credset -- not used in pipeline"
$gscp -r $staging/finemapping/$finemapping/credset/* $outputs/v2d_credset/

echo "Done downloading"

echo $"gsutil ls $inputs"