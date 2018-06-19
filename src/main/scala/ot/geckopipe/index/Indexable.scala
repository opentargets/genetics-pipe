package ot.geckopipe.index

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Indexable {
  def table: DataFrame

  /** simply select table using from seq of column names */
  def selectBy(from: Seq[String]): DataFrame = table.select(from.head, from.tail:_*)

  /** select the data from table using fromSelect columns grouped by cols
    *
    * it selects from fromSelect = Seq("c1", "c2", "c3", "c4", ...) and the cols = Seq("s1","s2")
    * and aggregate the result as first of each row resulting fromSelect diff cols = Seq("c3", "c4")
    *
    * @param cols col names used to groupby
    * @param fromSelect col names used to select the initial table
    * @return a select grouped and aggregated by fromSelect diff cols columns
    */
  def aggBy(cols: Seq[String], fromSelect: Seq[String]): DataFrame = {
    val aggCols = fromSelect diff cols map(el => col(el).as(el))

    selectBy(fromSelect)
      .groupBy(cols.head, cols.tail:_*)
      .agg(aggCols.head, aggCols.tail:_*)
  }
}
