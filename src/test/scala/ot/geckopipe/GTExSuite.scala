package ot.geckopipe

import minitest._

object GTExSuite extends SimpleTestSuite{
  test("extract filename from fullpath") {
    assertEquals(GTEx.extractFilename("/my/test/filename/Prueba.txt.gz"),"Prueba.txt.gz")
  }

  test("extract filename from path without /") {
    assertEquals(GTEx.extractFilename("Prueba.txt.gz"),"Prueba.txt.gz")
  }
}