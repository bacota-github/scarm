package com.vivi.scarm

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream

import scala.util.{Failure,Success,Try}

import scala.language.higherKinds

import cats.Id
import doobie._
import doobie.implicits._


sealed trait DatabaseObject {
  def create: Update0
  def drop: Update0
  def name: String
}


sealed trait Queryable[K, T, F[_]] {

  def keyNames: Seq[String]
  def sql: String = ""

  def doobieQuery(prefix: String, key: K)
    (implicit keyComposite: Composite[K], compositeT: Composite[T]) =
    Fragment(sql, key)(keyComposite).query[T](compositeT).process

  def query(key: K):  Stream[ConnectionIO,F[T]] = Stream()
}


sealed trait UniqueQueryable[K,T] extends Queryable[K,T,Id] {
  def +:[LK, LT, G[_]](query: Queryable[LK,LT,G], relation: Relation[LT,K,T]) =
    join(query,relation)
}

sealed trait OptionQueryable[K,T] extends Queryable[K,T,Option] {
  def +:[LK, LT, G[_]](query: Queryable[LK,LT,G], relation: Relation[LT,K,T]) =
    join(query,relation)
}

case class Relation[L, K, R](
  leftKey: L => K,
  rightKey: R => K,
  keyNames: Seq[(String,String)]
)


trait Entity[K] {
  def id: K
}

trait Table[K,E<:Entity[K]] extends DatabaseObject with UniqueQueryable[K,E] {

  override def key: E => K = { _.id }
  override def sql: String = primaryKey.sql

  override def create: Update0 = null
  override def drop: Update0 = null

  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this, (e: E) => e.id, keyNames)

  // def fetch(key: K): ConnectionIO[Option[E]] =
//    query(key).head
  def create(enties: E*): Try[Unit] = Success(Unit)
  //def createReturning(entities: E*): Try[Seq[E]]
  def delete(keys: K*): Try[Unit] = Success(Unit)
  def save(enties: E*): Try[Unit] = Success(Unit)
  //def savveReturning(entities: E*): Try[Seq[E]]
  def update(enties: E*): Try[Unit] = Success(Unit)
  //def updateReturning(entities: E*): Try[Seq[E]]
}


case class CompositeKeyTable[K,E<:Entity[K]](
  override val name: String,
  override val keyNames: Seq[String]
) extends Table[K,E]


case class IdTable[K<:AnyVal,E<:Entity[K]](
  override val name: String,
) extends Table[K,E] {
  override val keyNames: Seq[String] = Seq("id")
}


object Table {

  def apply[K,E<:Entity[K]](name:String, keyNames: Seq[String]) =
    CompositeKeyTable[K,E](name,keyNames)

  def apply[K<:AnyVal,E<:Entity[K]](name: String) = IdTable[K,E](name)
}


case class View[K,E,F[_]](
  override val name: String,
  override val sql: String,
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,E,F] {
  override def create: Update0 = null
  override def drop: Update0 = null 
}

case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,E,Set] {
  override def create: Update0 = null
  override def drop: Update0 = null
}

case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with OptionQueryable[K,E] {
  override def create: Update0 = null
  override def drop: Update0 = null
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends Queryable[FPK,TPK,F] {

  def from: Table[FPK,FROM]
  def fromKey: FROM => TPK
  def to: Table[TPK,TO]
  def keyNames: Seq[String]
  override def create: Update0 = null
  override def drop: Update0 = null
  override lazy val name: String = s"${from.name}_${to.name}_fk"
  lazy val manyToOne = Relation(fromKey, to.key, keyNames zip to.keyNames)
  lazy val oneToMany = Relation(to.key, fromKey, to.keyNames zip keyNames)

  lazy val index: Index[TPK,FPK,FROM] = Index(name+"_ix", from, fromKey, keyNames)

  def join[K, G[_]](query: Queryable[K,TO,G]): Queryable[K,(TO,Set[FROM]),G] =
    Join(query, oneToMany, index)

  def ::[K,G[_]](query: Queryable[K,TO,G]) = join(query)
}



case class MandatoryForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Id] {

  def +:[K,G[_]](query: Queryable[K,FROM,G]): Queryable[K, (FROM,Id[TO]), G] =
    Join (query, manyToOne, to)
}


case class OptionalForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Option] {
  def +:[K,G[_]](query: Queryable[K,FROM,G]): Queryable[K, (FROM,Option[TO]), G] =
    Join (query, manyToOne, to.primaryKey)
}


case class Join[LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]](
  left: Queryable[LEFTK,LEFTT,LEFTF],
  relation: Relation[LEFTT, RIGHTK, RIGHTT],
  right: Queryable[RIGHTK,RIGHTT,RIGHTF]
) extends Queryable[LEFTK, (LEFTT, RIGHTF[RIGHTT]), LEFTF] {
  override def keyNames = left.keyNames
}

/*
case class ManyToOneJoin[LEFTK, LEFTF[_], PK, MANY, ONE](
  left: Joinable[LEFTK,MANY,LEFTF],
  right: ManyToOne[MANY,PK,ONE]
) extends Joinable[LEFTK, MANY, LEFTF]
    with Queryable[LEFTK, (MANY,Id[ONE]), LEFTF] {

  override def query(key: LEFTK) = Stream()

  def identity[T](t: T) = t
  def join[K,F[_]](left: Joinable[K,LEFTK,F]) =
    Join[K,LEFTK,F, LEFTK,(MANY,Id[ONE]), LEFTF](left,identity(_),this)
  def ::[K,F[_]](left: Joinable[K,LEFTK,F]) = join(left)
}


case class OptionalManyToOneJoin[LEFTK, LEFTF[_], PK, MANY, ONE](
  left: Joinable[LEFTK,MANY,LEFTF],
  right: OptionalManyToOne[MANY,PK,ONE]
) extends Joinable[LEFTK, MANY, LEFTF]
    with Queryable[LEFTK, (MANY,Option[ONE]), LEFTF] {

  override def query(key: LEFTK) = Stream()

  def identity[T](t: T) = t
  def join[K,F[_]](left: Joinable[K,LEFTK,F]) =
    Join[K,LEFTK,F, LEFTK,(MANY,Option[ONE]), LEFTF](left,identity(_),this)
  def ::[K,F[_]](left: Joinable[K,LEFTK,F]) = join(left)
}


 */
