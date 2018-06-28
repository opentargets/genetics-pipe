package ot.geckopipe

import minitest.SimpleTestSuite

object FunctionsSuite extends SimpleTestSuite {
  val  groundtruth = Array("eqtl", "gtex_v7", "brain_temporal")

  test("function split and extract the path tokens correctly") {
    val path = "/foo/bar/qtl/eqtl/gtex_v7/Brain_Temporal/bar/foo"
    val triplet = functions.extractValidTokensFromPath(path, "/qtl/")

    assert(groundtruth.deep == triplet.deep)
  }

  test("function split from empty left ") {
    val path = "/qtl/eqtl/gtex_v7/Brain_Temporal/bar/foo"
    val triplet = functions.extractValidTokensFromPath(path, "/qtl/")

    assert(groundtruth.deep == triplet.deep)
  }

  test("function split but without '/' at the begining it will fail") {
    val path = "qtl/eqtl/gtex_v7/Brain_Temporal/bar/foo"
    val triplet = functions.extractValidTokensFromPath(path, "/qtl/")

    assert(groundtruth.deep != triplet.deep)
  }
}
