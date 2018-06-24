package com.vivi.scarm

import com.vivi.scarm._

import FieldMap._
import dslUtil._

case class View[K,E](
  val definition: String,
  override val keyNames: Seq[String],
  val fieldNames: Seq[String]
) extends Queryable[K,Set,E,E] {

  override private[scarm] def selectList(ct: Int): String = 
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def tablect: Int = 1

  override private[scarm] def tableList(ct: Int = 0): Seq[String] =
    Seq(alias(s"(${definition})", ct))

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows

  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet
}

object View {
  def apply[K,E](definition: String)
    (implicit fmap: FieldMap[E], kmap: FieldMap[K], config: ScarmConfig): View[K,E] = 
    View(definition,
      kmap.prefix(fmap.firstFieldName).names(config),
      fmap.names(config)
    )
}
