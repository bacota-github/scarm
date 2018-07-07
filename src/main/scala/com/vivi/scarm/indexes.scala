package com.vivi.scarm

import com.vivi.scarm._

import scala.language.higherKinds
import scala.reflect.runtime.universe.{Type,TypeTag,typeOf,Symbol=>RSymbol}

import cats.effect.IO
import cats.Id
import cats.data.NonEmptyList
import doobie._
import doobie.implicits._
import fs2.Stream
import shapeless._
import shapeless.ops.hlist

import FieldMap._
import dslUtil._

trait IndexBase[K,PK,E] {
  def name: String
  def table: Table[PK,E]
  def keyNames: Seq[String]
  def unique: Boolean

  def create: ConnectionIO[Int] = {
    val keys = keyNames.mkString(",")
    val uniqueness = if (unique) "UNIQUE" else ""
    val sql = s"CREATE ${uniqueness} INDEX ${name} on ${table.name} (${keys})"
    Fragment(sql, ()).update.run
  }
}

case class Index[K,PK,E](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E,E] with IndexBase[K,PK,E]{
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String ]= Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] = reduced.toSet
  override def unique: Boolean = false
}


object Index {

  private[scarm] def indexName(table: Table[_,_], ttag: TypeTag[_]) =
    table.name + "_" + ttag.tpe.typeSymbol.name + "_idx"

  private[scarm] def keyNames[K,E](table: Table[_,E])
    (implicit kmap: FieldMap[K], isProjection: Subset[K,E]) =
    table.fieldMap.keyMap(kmap).names(table.config)

  def apply[K,PK,E](table: Table[PK,E])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K]
  ): Index[K,PK,E] = Index(indexName(table,ktag), table, keyNames(table))

  def apply[K,PK,E](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K]
  ): Index[K,PK,E] = Index(indexName(table,ktag), table, keyNames(table))

  def apply[K,PK,E](name: String, table: Table[PK,E])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K]
  ): Index[K,PK,E] = Index(name, table, keyNames(table))

  def tupled[K,PK,E,KList<:HList, T<:Product](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K],
    kgen: Generic.Aux[K,KList],
    tupled: hlist.Tupler.Aux[KList,T],
  ): Index[T,PK,E] = Index(indexName(table,ktag), table, keyNames(table))
}


case class UniqueIndex[K,PK,E](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E,E] with IndexBase[K,PK,E] {
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String] = Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)

  override def unique: Boolean = true
}


object UniqueIndex {

  def apply[K,PK,E,KList<:HList,EList<:HList](table: Table[PK,E])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K]
  ): UniqueIndex[K,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, Index.keyNames(table))

  def apply[K,PK,E](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K]
  ): UniqueIndex[K,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, Index.keyNames(table))

  def apply[K,PK,E,KList<:HList,EList<:HList](name: String,  table: Table[PK,E])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K])
      : UniqueIndex[K,PK,E] = UniqueIndex(name, table, Index.keyNames(table))

  def tupled[K,PK,E,KList<:HList, T<:Product](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K],
    kgen: Generic.Aux[K,KList],
    tupled: hlist.Tupler.Aux[KList,T]
  ): UniqueIndex[T,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, Index.keyNames(table))
}

