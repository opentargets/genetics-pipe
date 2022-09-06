// import $ivy.`com.google.cloud:google-cloud-storage:2.4.0`
// import $ivy.`com.google.cloud:google-cloud-dataproc:2.3.2`
// https://mvnrepository.com/artifact/com.google.cloud/libraries-bom
//import $ivy.`com.google.cloud:libraries-bom:24.3.0`
// import $ivy.`io.grpc:grpc-protobuf:1.44.0`
// import $ivy.`com.google.protobuf:protobuf-java:3.19.3`

import com.google.cloud.dataproc.v1.{ClusterConfig, DiskConfig, GceClusterConfig, InstanceGroupConfig, ManagedCluster, OrderedJob, RegionName, SoftwareConfig, SparkJob, WorkflowTemplate, WorkflowTemplatePlacement, WorkflowTemplateServiceClient, WorkflowTemplateServiceSettings}

import scala.collection.JavaConverters.asJavaIterableConverter

val projectId = "open-targets-genetics-dev"
val region = "europe-west1"

val configPath = "gs://genetics-portal-dev-data/22.05.2/conf"
val jar = "gs://genetics-portal-dev-data/22.05.2/jars/etl-genetics-59c881b.jar"
val config = "2205_2.conf"
val workflow2Execute = "full"
require(Set("l2g", "full") contains workflow2Execute)

val gcpUrl = s"$region-dataproc.googleapis.com:443"


// Configure the settings for the workflow template service client.
val workflowTemplateServiceSettings =
  WorkflowTemplateServiceSettings.newBuilder.setEndpoint(gcpUrl).build

val workflowTemplateServiceClient: WorkflowTemplateServiceClient =
  WorkflowTemplateServiceClient.create(workflowTemplateServiceSettings)

// Configure the jobs within the workflow.
def sparkJob(step: String): SparkJob = SparkJob
  .newBuilder
  .setMainJarFileUri(jar)
  .addArgs(step)
  .addFileUris(s"$configPath/$config")
  .putProperties("spark.executor.extraJavaOptions", s"-Dconfig.file=$config")
  .putProperties("spark.driver.extraJavaOptions", s"-Dconfig.file=$config")
  .build

val variantIdx = "variant-index"
val variantDiseaseIdx = "variant-disease"
val variantGeneIdx = "variant-gene"
val scoredDatasetIdx = "scored-datasets"
val manhattanIdx = "manhattan"
val diseaseVariantGeneIdx = "disease-variant-gene"
val distanceIdx = "distance-nearest"
val dictionariesIdx = "dictionaries"
val colocIdx = "variant-disease-coloc"

val variantIndex: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(variantIdx)
  .setSparkJob(sparkJob(variantIdx))
  .build

val dictionaries: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(dictionariesIdx)
  .setSparkJob(sparkJob(dictionariesIdx))
  .addPrerequisiteStepIds(variantIdx)
  .build

val variantGene: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(variantGeneIdx)
  .setSparkJob(sparkJob(variantGeneIdx))
  .addPrerequisiteStepIds(variantIdx)
  .build

val variantDisease: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(variantDiseaseIdx)
  .setSparkJob(sparkJob(variantDiseaseIdx))
  .addPrerequisiteStepIds(variantIdx)
  .build

val variantDiseaseColoc: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(colocIdx)
  .setSparkJob(sparkJob(colocIdx))
  .addPrerequisiteStepIds(variantIdx)
  .build

val diseaseVariantGene: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(diseaseVariantGeneIdx)
  .setSparkJob(sparkJob(diseaseVariantGeneIdx))
  .addAllPrerequisiteStepIds(Seq(variantDiseaseIdx, variantGeneIdx).asJava)
  .build

val scoredDatasets: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(scoredDatasetIdx)
  .setSparkJob(sparkJob(scoredDatasetIdx))
  .addAllPrerequisiteStepIds(Seq(variantGeneIdx, diseaseVariantGeneIdx).asJava)
  .build

val manhattan: OrderedJob = OrderedJob
  .newBuilder
  .setStepId(manhattanIdx)
  .setSparkJob(sparkJob(manhattanIdx))
  .addAllPrerequisiteStepIds(Seq(colocIdx, scoredDatasetIdx).asJava)
  .build

// Configure the cluster placement for the workflow.// Configure the cluster placement for the workflow.
val gceClusterConfig = GceClusterConfig
  .newBuilder
  .setZoneUri(s"$region-d")
  .addTags("genetics-cluster")
  .build

val clusterConfig: ClusterConfig = {
  val softwareConfig = SoftwareConfig
    .newBuilder
    .setImageVersion("2.0-debian10")
    .putProperties("dataproc:dataproc.allow.zero.workers","true")
    .build

  val disk = DiskConfig
    .newBuilder
    .setBootDiskSizeGb(2000)
    .build

  val sparkMasterConfig = {
    InstanceGroupConfig
      .newBuilder
      .setNumInstances(1)
      .setMachineTypeUri("n1-highmem-96")
      .setDiskConfig(disk)
      .build
  }
  ClusterConfig
    .newBuilder
    .setGceClusterConfig(gceClusterConfig)
    .setSoftwareConfig(softwareConfig)
    .setMasterConfig(sparkMasterConfig)
    .build
}

val managedCluster = ManagedCluster.newBuilder.setClusterName("genetics-cluster").setConfig(clusterConfig).build
val workflowCluster = WorkflowTemplatePlacement.newBuilder.setManagedCluster(managedCluster).build

// Create the inline workflow template.
val workflowTemplate = WorkflowTemplate
  .newBuilder
  .addJobs(variantIndex)
  .addJobs(variantGene)



val parent = RegionName.format(projectId, region)

val l2gWorkflow = workflowTemplate.setPlacement(workflowCluster).build()
val fullWorkflow = workflowTemplate
  .addJobs(dictionaries)
  .addJobs(variantDisease)
  .addJobs(variantDiseaseColoc)
  .addJobs(diseaseVariantGene)
  .addJobs(scoredDatasets)
  .addJobs(manhattan)
  .setPlacement(workflowCluster)
  .build

val instantiateInlineWorkflowTemplateAsync = workflow2Execute match {
  case "l2g" =>
     workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(parent, l2gWorkflow)
  case "full" =>
    println("Preparing to run jobs:")
    fullWorkflow.getJobsList.forEach(it => println(it))
    workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(parent, fullWorkflow)
  case _ => throw new IllegalArgumentException("Unknown workflow selected.")
}

instantiateInlineWorkflowTemplateAsync.get

// Print out a success message.
println("Workflow ran successfully.")
workflowTemplateServiceClient.close()
