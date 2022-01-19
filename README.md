# Genetics-pipe


[![codecov](https://codecov.io/gh/opentargets/genetics-pipe/branch/master/graph/badge.svg)](https://codecov.io/gh/opentargets/genetics-pipe)
[![Build Status](https://travis-ci.com/opentargets/genetics-pipe.svg?branch=master)](https://travis-ci.com/opentargets/genetics-pipe)

**Everything is grch38 based**. ETL pipeline used to integrate and generate the following tables: 

- gene to variant
- disease to variant, and
- disease to variant to gene

## Howto build the pipeline

In order to use this pipeline the input data must follow an exact pattern described in these repositories

- [V2G](https://github.com/opentargets/g2v_data) Variant to gene data including tissues and cell-lines
- [V2D](https://github.com/opentargets/v2d_data) Variant to disease data

### Build the code

You only need `sbt >= 1.1.5`
 
```sh
sbt compile
sbt test
sbt assembly
```

Assembly command will generate a _fat-jar_ standalone _jar_ that you can run locally or submit to 
a spark cluster. This _jar_ already contains a default configuration file that you might want to copy
and edit for your own data.

To use your own configuration you need to pass `-f where/file/application.conf` to any executed command.

## Build a spark cluster on Google Cloud

To run the jar using Google's dataproc consult the script in `./scripts/run_cluster.sh` which will start a cluster 
and submit a separate job for each step.

## Configuration

The configuration is defined in `/src/main/resources/application.conf`. You can provide an external configuration 
with a subset of keys overwritten, for instance, only overwriting the keys specifying inputs. 

The following __inputs__ are required:

- `variant-index.raw`
- `ensembl.lut`
- `vep.homo-sapiens-cons-scores`
- `interval.path`
- `qtl.path`
- `variant-gene.weights`
- `variant-disease.studies`
- `variant-disease.toploci`
- `variant-disease.finemapping`
- `variant-disease.ld`
- `variant-disease.overlapping`
- `variant-disease.coloc`
- `variant-disease.trait_efo`

For running internally within Open Targets consult the [additional documentation](documentation/ot_genetics_deployment.md#Overview).


## Variant index generation

You will need vep consequences file from ensembl ftp

- unzip it
- split into more or equal to 64 pieces

Example command to run in order to split the vep file

```sh
split -a 3 --additional-suffix=vcf -d -n l/64 vep_csq_file.vcf vep_
``` 

## Variant to Gene table

TBD

## Variant to disease table

TBD

## Disease to variant to gene table

This table is a inner join between _v2g_ and _v2d_

# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
