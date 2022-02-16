import com.google.cloud.storage.{Blob, CopyWriter, Storage, StorageOptions}
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Storage.BlobListOption

import scala.collection.JavaConverters

/**
  * Run using Ammonite: checks whether the dirs in expected dirs are present in the output directory.
  * Remember some files are not generated by the pipeline but are static: sa and v2d_credset.
  */

object DataVersions {
  val stagingBucket = "genetics-portal-dev-staging"
  val outputBucket = "genetics-portal-dev-data"
  val sumstatsBucket = "genetics-portal-dev-sumstats"

  val release = "22.02"
  val BLut = "220105"
  val traitEfo = "2021-01-14"
  val v2dVersion = "220208"
  val coloc = "220113_merged"
  val finemapping = "210923"
  val qtl = "220105"
  val gwas = "220208"
  val molecularTrait = "220105"
  // There are a number of inputs we recycle from release to release.
  val oldInputs = s"$outputBucket/21.10/inputs"
}

object GeneticsInput {

  lazy val storage: Storage = StorageOptions.getDefaultInstance.getService
  val v2dInOut = Seq(
    (s"${DataVersions.stagingBucket}/v2d/${DataVersions.v2dVersion}/studies.parquet", s"${DataVersions.outputBucket}/v2d/"),
    (s"${DataVersions.stagingBucket}/v2d/${DataVersions.v2dVersion}/studies.parquet", s"${DataVersions.outputBucket}/v2d/"),
    (s"${DataVersions.stagingBucket}/v2d/${DataVersions.v2dVersion}/studies.parquet", s"${DataVersions.outputBucket}/v2d/"),
    (s"${DataVersions.stagingBucket}/v2d/${DataVersions.v2dVersion}/studies.parquet", s"${DataVersions.outputBucket}/v2d/"),
  )
  val inOutSeq: Seq[(String, String)] = Seq(
    (s"${DataVersions.oldInputs}/lut/biofeature_labels.json", s"${DataVersions.outputBucket}/lut/biofeature_labels.json"),
    (s"${DataVersions.oldInputs}/lut/vep_consequences.tsv", s"${DataVersions.outputBucket}/lut/vep_consequences.tsv"),
    (s"${DataVersions.oldInputs}/lut/v2g_scoring_source_weights.141021.json", s"${DataVersions.outputBucket}/lut/v2g_scoring_source_weights.141021.json"),
    (s"${DataVersions.stagingBucket}/lut/biofeature_labels/${DataVersions.BLut}/biofeature_labels.w_composites.json", s"${DataVersions.outputBucket}/lut/biofeature_lut_${DataVersions.BLut}.w_composites.json"),
  )

  def pathToBucketBlob(path: String): (String, String) = {
    val bucketAndBlob = path.splitAt(path.indexOf('/'))
    (bucketAndBlob._1, bucketAndBlob._2.drop(1)) // remove leading '/'
  }

  def inputsPresent(): Boolean = {

    val validatedInputs = inOutSeq.map(_._1).map(in => {
      println(s"Validating $in")
      val (bucket, blob) = pathToBucketBlob(in)
      println(s"$bucket, $blob")
      val blobs: Page[Blob] =
        storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(blob))
      val blobsFound: Set[String] = JavaConverters
        .asScalaIteratorConverter(blobs.iterateAll().iterator())
        .asScala
        .map(_.getName)
        .toSet
      println(blobsFound)
      (in, !blobsFound.contains(blob))
    }).filter(_._2)

    validatedInputs.foreach(i => println(s"${i._1} not found."))
    validatedInputs.isEmpty
  }

  def copy(fromBucket: String, toBucket: String, srcName: String, dstName: String): Unit = {

    val blob: Option[Blob] = Option(storage.get(fromBucket, srcName))

    blob.fold({ println(s"$fromBucket/$srcName not found.") })(b => {
      println(s"Copying $fromBucket/$srcName to $toBucket/$dstName")
      b.copyTo(toBucket, dstName)
    })
  }
}
GeneticsInput.inputsPresent()