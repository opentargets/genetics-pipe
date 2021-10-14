#!/bin/bash

release="21.10"

# --single-node
gcloud beta dataproc clusters create \
        etl-cluster-genetics-mk-96 \
        --image-version=2.0-debian10 \
        --region=europe-west1 \
        --zone=europe-west1-d \
        --master-machine-type=n1-highmem-96 \
        --master-boot-disk-size=2000 \
        --single-node \
        --project=open-targets-genetics-dev \
        --initialization-action-timeout=20m \
        --max-idle=30m

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "variant-index"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "dictionaries"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "variant-disease-coloc"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "variant-disease"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --properties='spark.sql.autoBroadcastJoinThreshold=-1' \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "variant-gene"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "variant-gene-scored"

gcloud dataproc jobs submit spark \
     --cluster=etl-cluster-genetics-mk-96 \
     --project=open-targets-genetics-dev \
     --region=europe-west1 \
     --async \
     --jar=gs://genetics-portal-dev-data/${release}/ot-geckopipe-assembly-latest.jar \
     -- "disease-variant-gene"
