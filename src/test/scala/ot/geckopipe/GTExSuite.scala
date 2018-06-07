package ot.geckopipe

import minitest._
import ot.geckopipe.positional.GTEx

object GTExSuite extends SimpleTestSuite{
  test("extract filename from fullpath") {
    assertEquals(GTEx.extractFilename("/my/test/filename/Prueba.txt.gz"),"Prueba.txt.gz")
  }

  test("extract filename from path without /") {
    assertEquals(GTEx.extractFilename("Prueba.txt.gz"),"Prueba.txt.gz")
  }
}