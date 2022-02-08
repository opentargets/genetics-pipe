#!/bin/bash

set -x

release="22.02.1"
config=2202_1.conf

jarVersion=$(git rev-parse --short HEAD)
image=2.0.28-debian10

cluster_name=genetics-pipeline-cluster
path=gs://genetics-portal-dev-data/${release}
config_path=$path/conf
jarfile=ot-pipe-$jarVersion.jar
## If you want to run a pre-uploaded jar uncomment the following line and provide the correct name.
jarfile=ot-pipe-a09fb88.jar
jartoexecute=gs://genetics-portal-dev-data/$release/jars/$jarfile

#sbt 'set test in assembly := {}' clean assembly
#
#version=$(awk '$1 == "version" {print substr($NF, 2, length($NF)-2)}' build.sbt)
#gsutil -m cp -n target/scala-2.12/ot-*$version.jar $jartoexecute

# --single-node
gcloud beta dataproc clusters create \
  $cluster_name \
  --image-version=$image \
  --region=europe-west1 \
  --zone=europe-west1-d \
  --master-machine-type=n1-highmem-96 \
  --master-boot-disk-size=2000 \
  --single-node \
  --project=open-targets-genetics-dev \
  --initialization-action-timeout=20m \
  --max-idle=30m

tasks=(
  'variant-index'
  'variant-gene'
  'dictionaries'
  'variant-disease'
  'distance-nearest'
  'variant-disease-coloc'
  'disease-variant-gene'
  'scored-datasets'
  'manhattan'
)
for t in "${tasks[@]}"; do
  echo "Starting task $t on $cluster_name"
  gcloud dataproc jobs submit spark \
    --cluster=$cluster_name \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --files="$config_path$config" \
    --properties=spark.executor.extraJavaOptions=-Dconfig.file=$config,spark.driver.extraJavaOptions=-Dconfig.file=$config \
    --labels=task=$t \
    --jar=$jartoexecute \
    -- $t
done
