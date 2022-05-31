package ot.geckopipe.index

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import ot.geckopipe.functions

object Indexable {

  implicit class DataFrameIndexable(df: DataFrame) {
    def selectBy(from: Seq[String]): DataFrame = df.select(from.head, from.tail: _*)

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
      val aggCols = fromSelect diff cols map (el => first(el).as(el))

      selectBy(fromSelect)
        .groupBy(cols.head, cols.tail: _*)
        .agg(aggCols.head, aggCols.tail: _*)
    }

    /** save the dataframe as tsv file using filename as a output path */
    def savetoCSV(to: String): Unit = functions.saveToCSV(df, to)

    /** save the dataframe as tsv file using filename as a output path */
    def saveToJSON(to: String): Unit = functions.saveToJSON(df, to)
  }

}
