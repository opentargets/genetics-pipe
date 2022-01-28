import $ivy.`com.google.cloud:google-cloud-storage:2.3.0`

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BlobListOption

import scala.collection.JavaConverters

/**
  * Run using Ammonite: checks whether the dirs in expected dirs are present in the output directory.
  * Remember some files are not generated by the pipeline but are static: sa and v2d_credset.
  */
val outputDir = "gs://genetics-portal-dev-data/22.01.2/outputs/"

val expectedDirs =
  Set("d2v2g",
    "d2v2g_scored",
    "l2g",
    "lut",
    "manhattan",
    "sa",
    "v2d",
    "v2d_coloc",
    "v2d_credset",
    "v2g",
    "v2g_scored",
    "variant-index")

/**
  * @path path to google storage blob.
  * @return a tuple of (bucket, blob) for a given path.
  * */
def pathToBucketBlob(path: String): (String, String) = {
  require(path.trim.startsWith("gs://"))
  val noPrefix = path.drop(5)
  val bucketAndBlob = noPrefix.splitAt(noPrefix.prefixLength(_ != '/'))
  (bucketAndBlob._1, bucketAndBlob._2.drop(1)) // remove leading '/'
}

val storage: Storage = StorageOptions.getDefaultInstance.getService


val (bucket, blob) = pathToBucketBlob(outputDir)

val blobs: Page[Blob] =
  storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(blob))

val blobsFound: Set[String] = JavaConverters
  .asScalaIteratorConverter(blobs.iterateAll().iterator())
  .asScala
  .filter(_.isDirectory)
  .map(_.getName)
  .toSet

val pattern = "([^\\/]+)\\/?$".r
val directoriesFound = blobsFound.flatMap(pattern.findFirstIn(_)).map(_.dropRight(1))

val missing = expectedDirs diff directoriesFound
println(s"Missing directories: $missing")