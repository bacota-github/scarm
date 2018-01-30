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
  def create: Update0
  def drop: Update0
  def name: String
}


sealed trait Queryable[K, F[_], E] {

  def keyNames: Seq[String]
  def joinKeyNames: Seq[String] = keyNames
  def sql: String = ""

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

//  override def key: E => K = { _.id }
  override def sql: String = primaryKey.sql

  override def create: Update0 = null
  override def drop: Update0 = null

//  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this, (e: E) => e.id, keyNames)
  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this, keyNames)

  // def fetch(key: K): ConnectionIO[Option[E]] =
//    query(key).head
  def create(enties: E*): Try[Unit] = Success(Unit)
  //def createReturning(entities: E*): Try[Seq[E]]
  def delete(keys: K*): Try[Unit] = Success(Unit)
  def save(enties: E*): Try[Unit] = Success(Unit)
  //def saveReturning(entities: E*): Try[Seq[E]]
  def update(enties: E*): Try[Unit] = Success(Unit)
  //def updateReturning(entities: E*): Try[Seq[E]]
}

case class View[K,E](
  override val name: String,
  override val sql: String,
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E] {
  override def create: Update0 = null
  override def drop: Update0 = null 
}

case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
//  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E] {
  override def create: Update0 = null
  override def drop: Update0 = null
}

case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
//  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E] {
  override def create: Update0 = null
  override def drop: Update0 = null
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends DatabaseObject
    with Queryable[FROM, F, TO] {

  def from: Table[FPK,FROM]
  def fromKey: FROM => TPK
  def to: Table[TPK,TO]
  override def keyNames: Seq[String]
  override def joinKeyNames: Seq[String] = to.keyNames

  override def create: Update0 = null
  override def drop: Update0 = null
  override lazy val name: String = s"${from.name}_${to.name}_fk"

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
}


case class Join[K,LF[_], JK, RF[_],E](
  left: Queryable[K,LF,JK],
  right: Queryable[JK,RF,E]
) extends Queryable[K,LF,(JK,RF[E])] {
  override def keyNames = left.keyNames
  override def joinKeyNames = right.joinKeyNames
}


case class NestedJoin[K,LF[_], JK,X, RF[_],E](
  left: Queryable[K,LF,(JK,X)],
  right: Queryable[JK,RF,E]
) extends Queryable[K,LF,(JK,X,RF[E])] {
  override def keyNames = left.keyNames
  override def joinKeyNames = left.joinKeyNames
}

