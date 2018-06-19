package ot.geckopipe

object Chromosomes {
  // Seq[String] of crhomosomes including X Y and MT
  val chrList: Seq[String] = (1 to 22).map(_.toString) ++ Seq("X", "Y", "MT")
  // String built to use 'in ()' SQL section
  val chrString: String = chrList.mkString("('", "', '", "')")
}
