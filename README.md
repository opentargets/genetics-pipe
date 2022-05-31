# Genetics-pipe

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

The `sbt assembly` command will generate a _fat-jar_ standalone _jar_ that you can run locally or submit to 
a spark cluster. This _jar_ already contains a default configuration file (`src/main/resources/reference.conf`) 
which includes present values. The configuration library follows Typesafe's documented [configuration priority 
settings](https://github.com/lightbend/config#standard-behavior). In short, to add your own configuration file from 
the command line use `-Dconfig.file=path/to/config-file`. Any fields not present in that file will be resolved using 
the `reference.conf` file. 

An example configuration file is provided under `configuration/example.conf` which lists all of the inputs which 
need to be specified. Updating this file is sufficient to run a genetics release. 

## Build a spark cluster on Google Cloud

To run the jar using Google's dataproc consult the script in `./scripts/run_cluster.sh` which will start a cluster 
and submit a separate job for each step. The steps need to be executed in the correct order, so don't run 
asynchronously or change the specified order of the steps.

## Run using a Google Dataproc Workflow

The _simplest_ way to run everything is using the workflow defined in `./scripts/dataproc-workflow.sc`. From within 
Intellij you can run it as a worksheet. The workflow defines each of the steps as a Dataproc job and runs it on a 
cluster. The cluster is automatically started and stopped, and jobs are parallelized where possible. 

Alternatively you can run it from Ammonite but you need to ensure that all of the required jars are on the classpath.
This can be done by executing the following command `sbt Compile/fullClasspath/exportToAmmoniteScript && amm 
--predef target/scala-2.12/fullClasspath-Compile.sc`. From here you can copy/paste the workflow into Ammonite. 

## Configuration

The configuration is defined in `/src/main/resources/reference.conf`. You can provide an external configuration 
with a subset of keys overwritten, for instance, only overwriting the keys specifying inputs. 

The following __inputs__ are required:

- `variant-index.raw`
- `ensembl.lut`
- `vep.homo-sapiens-cons-scores`
- `interval.path`
- `qtl.path`
- ~~variant-gene.weights~~ Deprecated as of 30 May 2022. Weights should be specified in the configuration field 
  `variant-gene.weights`
- `variant-disease.studies`
- `variant-disease.toploci`
- `variant-disease.finemapping`
- `variant-disease.ld`
- `variant-disease.overlapping`
- `variant-disease.coloc`
- `variant-disease.trait_efo`

For running internally within Open Targets consult the [additional documentation](documentation/ot_genetics_deployment.md#Overview).

### Running steps

| Step name | Dependencies | Output |
| --- | --- | --- |
| `variant-gene` |  | `v2g` |
| `variant-index` |  | `variant-index` |
| `dictionaries` | `variant-index` | `lut` |
| `variant-disease` | `variant-index` | `v2d` |
| `variant-disease-coloc` | `variant-index` | `v2d_coloc` |
| `distance-nearest` | `variant-index` | `distance/canonical_tss` |
| `disease-variant-gene` |  `variant-disease` | `d2v2g` |
| `scored-datasets` | `variant-gene`, `disease-variant-gene` | `v2g_by_overall`, `v2g_scored`, `d2v2g_by_overall`, `d2v2g_scored` |
| `manhattan` | `l2g`, `scored-datasets`, `variant-disease-coloc` | `manhattan` |

For a graphical representation of the dependencies see `./documentation/step_dependencies.puml`

## Variant to Gene table

### Inputs

| conf field | notes |
| --- | --- |
| conf.vep.homoSapiensConsScores | Notes taken directly from comment in code (might not be correct) load consequence table from file extracted from ensembl website https://www.ensembl.org/info/genome/variation/predicted_data.html#consequences and merged with OT eco scores table. We filter by only v2g_scores and get last token from the accession terms |
| conf.ensembl.lut | Creates a gene index |
| conf.variantGene.path | This is the output of the `variant-gene` step, so we assume there is a dependency between them. |

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
