# Geckopipe

[![Build Status](https://travis-ci.com/opentargets/geckopipe.svg?branch=master)](https://travis-ci.com/opentargets/geckopipe)

**Everything is grch37 based**. ETL pipeline used to integrate and generate the following tables: 

- gene to variant
- disease to variant, and
- disease to variant to gene

## Howto build the pipeline

In order to use this pipeline the input data must follow an exact pattern described in these repositories

- [V2G](https://github.com/opentargets/g2v_data) Variant to gene data including tissues and cell-lines
- [V2D](https://github.`com/opentargets/v2d_data) Variant to disease data

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