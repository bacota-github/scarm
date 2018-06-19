package com.vivi.scarm

import com.vivi.scarm._


case class TableScan[K,E](table: Table[K,E])
    extends Queryable[Unit, Set, E, E] {
  def keyNames = Seq()
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int): Seq[String] = table.tableList(ct)
  override private[scarm] def tablect: Int = 1
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] =
    table.reduceResults(rows)
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet

  override lazy val sql: String = s"SELECT ${selectList(1)} FROM ${tableList(1).head}"
}
