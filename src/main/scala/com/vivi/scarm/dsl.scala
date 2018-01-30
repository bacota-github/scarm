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


sealed trait Queryable[K, T, E, J[_]] {

  def keyNames: Seq[String]
  def joinKeyNames: Seq[String] = keyNames
  def sql: String = ""

  def doobieQuery(prefix: String, key: K)
    (implicit keyComposite: Composite[K], compositeT: Composite[T]) =
    Fragment(sql, key)(keyComposite).query[T](compositeT).process

  def query(key: K):  Stream[ConnectionIO,F[T]] = Stream()

  def join[LEFTK,LEFTT, LEFTJ[_]](
    query: Queryable[LEFTK,LEFTT,K,LEFTJ]
  ): Queryable[LEFTK,(LEFTT,LEFTJ[T]),E,
    ({type L[X] = (LEFTT,X)})#L] =
    Join(query,this)

  def ::[LEFTK,LEFTT, LEFTJ[_]](query: Queryable[LEFTK,LEFTT,K,LEFTJ]) = join(query)
}



case class Table[K,E<:Entity[K]](
  override val name: String,
  override val keyNames: Seq[String] = Seq("id")
) extends DatabaseObject with Queryable[K,E,E,Id] {

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
) extends DatabaseObject with Queryable[K,E,E,Option] {
  override def create: Update0 = null
  override def drop: Update0 = null 
}

case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
//  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,E,E,Set] {
  override def create: Update0 = null
  override def drop: Update0 = null
}

case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
//  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,E,E,Option] {
  override def create: Update0 = null
  override def drop: Update0 = null
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends DatabaseObject
    with Queryable[FROM, F[TO], TO, F] {

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
) extends Queryable[FROM, Set[TO], TO, Set] {

  override def keyNames: Seq[String] = foreignKey.to.keyNames
  override def joinKeyNames: Seq[String] = foreignKey.from.keyNames
}


case class Join[LEFTK,LEFTT,LEFTJ[_], RIGHTK,RIGHTT,RIGHTJ[_], E](
  left: Queryable[LEFTK,LEFTT,RIGHTK,LEFTJ],
  right: Queryable[RIGHTK,RIGHTT,E,RIGHTJ]
) extends
    Queryable[LEFTK,(LEFTT,LEFTJ[RIGHTT]),E,
      ({type L[X] = (LEFTT,X)})#L] {
  override def keyNames = left.keyNames
  override def joinKeyNames = right.joinKeyNames
}

