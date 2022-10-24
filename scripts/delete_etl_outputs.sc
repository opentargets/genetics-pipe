
import sys.process._
import scala.language.postfixOps

val out = "gs://genetics-portal-dev-data/22.09.1/outputs"
println(s"Getting outputs from $out")
val files = {s"gsutil ls $out" !! } split "\\n"

val staticFiles = Set("l2g", "sa", "v2d_credset")

val filesToDelete = files.filter(f => {
  val suffix = f.split('/').last
  !staticFiles.contains(suffix)
})
println(s"Preparing to delete:")
filesToDelete.foreach(f => println(s"\t * $f"))

val deleteResults = filesToDelete.map(f => {
  println(s"Deleting $f")
  (f, s"gsutil -m rm -r $f" !)
} )
println("Checking for errors: (should be empty)")
deleteResults.filter(_._2 != 0).foreach(r => println(s"Failed to delete ${r._1}"))

println("Process complete")
