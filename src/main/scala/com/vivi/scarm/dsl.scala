package com.vivi.scarm

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream

import scala.util.{Failure,Success,Try}

import scala.language.higherKinds

import cats.Id
import doobie._
import doobie.implicits._


trait Entity[K] {
  def id: K
}


sealed trait DatabaseObject {
  def name: String

  def tname(ct: Int): String = s"${name}${ct}"
  def selectList(ct: Int): String = tname(ct) + ".*"
  def tableList(ct: Int): String = s"${name} AS ${tname(ct)}"
}


sealed trait Queryable[K, F[_], E] {

  def keyNames: Seq[String]
  def joinKeyNames: Seq[String] = keyNames

  def selectList(ct: Int=0): String
  def tableList(ct: Int=0): String
  def whereClause: String = keyNames.map(_ + "=?").mkString(" AND ")


  def sql: String =
    s"SELECT ${selectList(1)} FROM ${tableList(1)} WHERE ${whereClause}"

  def doobieQuery(prefix: String, key: K)
    (implicit keyComposite: Composite[K], compositeT: Composite[F[E]]) =
    Fragment(sql, key)(keyComposite).query[F[E]](compositeT).process

  def query(key: K):  Stream[ConnectionIO,F[E]] = Stream()

  def join[LK,LF[_]](query: Queryable[LK,LF,K]): Queryable[LK,LF,(K,F[E])] =
    Join(query,this)

  def ::[LK,LF[_]](query: Queryable[LK,LF,K]): Queryable[LK,LF,(K,F[E])] =
    join(query)

  def nestedJoin[LK,LF[_],X](query: Queryable[LK,LF,(K,X)])
        :Queryable[LK,LF,(K,X,F[E])] = NestedJoin(query,this)

  def :::[LK,LF[_],X](query: Queryable[LK,LF,(K,X)])
      :Queryable[LK,LF,(K,X,F[E])] = nestedJoin(query)
}


case class Table[K,E<:Entity[K]](
  override val name: String,
  override val keyNames: Seq[String] = Seq("id")
) extends DatabaseObject with Queryable[K,Id,E] {

  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this, keyNames)

  // def fetch(key: K): ConnectionIO[Option[E]] =
//    query(key).head
  //def createReturning(entities: E*): Try[Seq[E]]
  def delete(keys: K*): Try[Unit] = Success(Unit)
  def save(enties: E*): Try[Unit] = Success(Unit)
  //def saveReturning(entities: E*): Try[Seq[E]]
  def update(enties: E*): Try[Unit] = Success(Unit)
  //def updateReturning(entities: E*): Try[Seq[E]]
}

case class View[K,E](
  override val name: String,
  val definition: String,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E] {
  override def tableList(ct: Int = 0): String = s"(${definition}) AS ${tname(ct)}"
}

case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E] {
  override def tableList(ct: Int=0): String = s"${table.name} AS ${tname(ct)}"
}

case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E] {
  override def tableList(ct: Int=0): String = s"${table.name} AS ${tname(ct)}"
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends DatabaseObject
    with Queryable[FROM, F, TO] {

  def from: Table[FPK,FROM]
  def fromKey: FROM => TPK
  def to: Table[TPK,TO]
  override def keyNames: Seq[String]
  override def joinKeyNames: Seq[String] = to.keyNames

  override lazy val name: String = s"${from.name}_${to.name}_fk"

  override def selectList(ct: Int): String = to.selectList(ct)
  override def tableList(ct: Int): String = s"${to.name} AS ${tname(ct)}"

  lazy val oneToMany = ReverseForeignKey(this)
  lazy val reverse = oneToMany

  lazy val index: Index[TPK,FPK,FROM] = Index(name+"_ix", from, keyNames)
}


case class MandatoryForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Id]


case class OptionalForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Option]


case class ReverseForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK],F[_]](
  val foreignKey: ForeignKey[TPK,TO,FPK,FROM,F]
) extends Queryable[FROM, Set, TO] {
  override def keyNames: Seq[String] = foreignKey.to.keyNames
  override def joinKeyNames: Seq[String] = foreignKey.from.keyNames
  override def selectList(ct: Int): String = foreignKey.to.selectList(ct)
  override def tableList(ct: Int): String = s"${foreignKey.to.name} AS ${foreignKey.tname(ct)}"
}


case class Join[K,LF[_], JK, RF[_],E](
  left: Queryable[K,LF,JK],
  right: Queryable[JK,RF,E]
) extends Queryable[K,LF,(JK,RF[E])] {

  override def keyNames = left.keyNames

  override def joinKeyNames = right.joinKeyNames

  override def selectList(ct: Int): String =
    left.selectList(ct) +","+ right.selectList(ct+1)

  override def tableList(ct: Int): String =
    left.tableList(ct) +","+ right.selectList((ct+1))
}


case class NestedJoin[K,LF[_], JK,X, RF[_],E](
  left: Queryable[K,LF,(JK,X)],
  right: Queryable[JK,RF,E]
) extends Queryable[K,LF,(JK,X,RF[E])] {

  override def keyNames = left.keyNames

  override def joinKeyNames = left.joinKeyNames

  override def selectList(ct: Int): String =
    s"J${ct}.*," + right.selectList(ct+1)

  override def tableList(ct: Int): String =
    s"(${left.sql}) J${ct}, " + right.tableList(ct+1)
}

