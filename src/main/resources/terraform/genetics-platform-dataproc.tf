variable "version-suffix" {
  default = "0-22-4"
  type = "string"
}

variable "region" {
  default = "europe-west1"
  type = "string"
}

// Configure the Google Cloud provider
provider "google" "google-account" {
  credentials = "${file("open-targets-genetics-63ea40a7fb68.json")}"
  project     = "open-targets-genetics"
  region      = "${var.region}"
}

resource "google_storage_bucket" "gp-bucket" {
  name = "genetics-portal-cluster-bucket-${var.version-suffix}"
  location = "${var.region}"
  storage_class = "regional"
  force_destroy = true
}

resource "google_dataproc_cluster" "gp-cluster" {
    name       = "cluster-${var.version-suffix}"
    region     = "${var.region}"
#    labels {
#        foo = "bar"
#    }

    cluster_config {
        staging_bucket        = "${google_storage_bucket.gp-bucket.name}"

        master_config {
            num_instances     = 1
            machine_type      = "n1-highmem-32"
            disk_config {
                boot_disk_size_gb = 500
            }
        }

        worker_config {
            num_instances     = 3
            machine_type      = "n1-highmem-32"
            disk_config {
                boot_disk_size_gb = 500
                # num_local_ssds    = 0
            }
        }

        preemptible_worker_config {
            num_instances     = 0
        }

        # Override or set some custom properties
        software_config {
            image_version       = "preview"
            override_properties = {
                "dataproc:dataproc.allow.zero.workers" = "true"
            }
        }

        gce_cluster_config {
            # network = "${google_compute_network.dataproc_network.name}"
            # tags    = ["foo", "bar"]
        }

        # You can define multiple initialization_action blocks
#        initialization_action {
#            script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
#            timeout_sec = 500
#        }

    }
}

# Submit an example spark job to a dataproc cluster
# resource "google_dataproc_job" "gp-spark" {
#     region       = "${var.region}"
#     # force_delete = true
#     placement {
#         cluster_name = "${google_dataproc_cluster.gp-cluster.name}"
#     }
# 
#     spark_config {
#         jar_file_uris = ["file:///usr/lib/spark/examples/jars/spark-examples.jar"]
#         args          = ["variant-index"]
# 
#         properties    = {
#             "spark.logConf" = "true"
#         }
# 
#         logging_config {
#             driver_log_levels {
#                 "root" = "INFO"
#             }
#         }
#     }
# }
