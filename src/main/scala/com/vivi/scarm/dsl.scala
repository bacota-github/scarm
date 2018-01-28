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


sealed trait Relation[L, K, R] {
  def leftKey: L => K
  def rightKey: R => K
  def keyPairs: Seq[(String,String)]
}


sealed trait Joinable[L,K,R,F[_]] {
  def relation: Relation[L,K,R]
  def rightQuery: Queryable[K,R,F]

  def join[K2, G[_]](query: SimpleQueryable[K2,L,G]): SimpleQueryable[K2,(L,F[R]),G] =
    SimpleJoin(query,relation,rightQuery)

  def ::[LK, G[_]](query: SimpleQueryable[LK,L,G]) = join(query)

  def join[LK, L2,K2,T,G[_]](query: JoinableQueryable[LK,L2,K2,L,T,G]):
      JoinableQueryable[LK,L2,K2,L,(T,F[R]),G]  =
    JoinableQueryable(query.relation, 

  def ::[LK, L2,K2,T,G[_]](query: JoinableQueryable[LK,L2,K2,L,T,G]) = join(query)
}


sealed trait SimpleQueryable[K,T,F[_]]
    extends Queryable[K,T,F]

sealed trait JoinableQueryable[LK,L,K,R,T,F[_]]
    extends Queryable[LK,T,F] with Joinable[L,K,R,F]


trait Entity[K] {
  def id: K
}

trait Table[K,E<:Entity[K]] extends DatabaseObject with SimpleQueryable[K,E,Id] {

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
) extends DatabaseObject with SimpleQueryable[K,E,F] {
  override def create: Update0 = null
  override def drop: Update0 = null 
}

case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with SimpleQueryable[K,E,Set] {
  override def create: Update0 = null
  override def drop: Update0 = null
}

case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  override val key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with SimpleQueryable[K,E,Option] {
  override def create: Update0 = null
  override def drop: Update0 = null
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
extends Relation[FROM,TPK,TO] {
  def from: Table[FPK,FROM]
  def fromKey: FROM => TPK
  def to: Table[TPK,TO]
  def keyNames: Seq[String]

  override def leftKey = fromKey
  override def rightKey = { _.id }
  override lazy val keyPairs = keyNames zip to.keyNames

  override def create: Update0 = null
  override def drop: Update0 = null
  override lazy val name: String = s"${from.name}_${to.name}_fk"

  lazy val oneToMany = ReverseForeignKey(this)

  lazy val index: Index[TPK,FPK,FROM] = Index(name+"_ix", from, fromKey, keyNames)
}


case class MandatoryForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Id] {

  def join[K, G[_]](query: SimpleQueryable[K,FROM,G]): SimpleQueryable[K,(FROM,Id[TO]),G] =
    SimpleJoin(query, this, to)

  def ::[K,G[_]](query: SimpleQueryable[K,FROM,G]) = join(query)
}


case class OptionalForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Option] {

  def join[K, G[_]](query: SimpleQueryable[K,FROM,G]): SimpleQueryable[K,(FROM,Option[TO]),G] =
    SimpleJoin(query, this, to.primaryKey)

  def ::[K,G[_]](query: SimpleQueryable[K,FROM,G]) = join(query)
}


case class ReverseForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK],F[_]](
  val foreignKey: ForeignKey[FPK,FROM,TPK,TO,F]
) extends Relation[TO,TPK,FROM] {
  override def leftKey = foreignKey.rightKey
  override def rightKey = foreignKey.leftKey
  override lazy val keyPairs = foreignKey.to.keyNames zip foreignKey.keyNames

  def join[K, G[_]](query: SimpleQueryable[K,TO,G]): SimpleQueryable[K,(TO,Set[FROM]),G] =
    SimpleJoin(query, this, foreignKey.index)

  def ::[K,G[_]](query: SimpleQueryable[K,TO,G]) = join(query)
}


sealed trait Join[LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]] {
  override def keyNames = left.keyNames
  def left: SimpleQueryable[LEFTK,LEFTT,LEFTF]
  def relation: Relation[LEFTT, RIGHTK, RIGHTT]
  def right: Queryable[RIGHTK,RIGHTT,RIGHTF]
}


case class SimpleJoin[LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]](
  override val left: SimpleQueryable[LEFTK,LEFTT,LEFTF],
  override val relation: Relation[LEFTT, RIGHTK, RIGHTT],
  override val right: Queryable[RIGHTK,RIGHTT,RIGHTF]
) extends Join[LEFTK,LEFTT,LEFTF, RIGHTK,RIGHTT,RIGHTF]
    with SimpleQueryable[LEFTK, (LEFTT, RIGHTF[RIGHTT]), LEFTF]



case class JoinableJoin[K, T, LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]](
  leftRelation: Relation[T, LEFTK, LEFTT],
  override val left: JoinableQueryable[K,T,LEFTK,LEFTT,RIGHTT,LEFTF]
  override val relation: Relation[LEFTT, RIGHTK, RIGHTT],
  override val right: Queryable[RIGHTK,RIGHTT,RIGHTF]
) extends Join[LEFTK,LEFTT,LEFTF, RIGHTK,RIGHTT,RIGHTF]
    with JoinableQueryable[K, T, LEFTK, RIGHTT, (LEFTT,RIGHTF[RIGHTT]), LEFTF]



//case class CompoundJoin
/*

case class JoinableJoin[
  FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_],
  LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]](
  foreignKey: ForeignKey[FPK,FROM,TPK,TO,F],
  left: SimpleQueryable[LEFTK,LEFTT,LEFTF],
  relation: Relation[LEFTT, RIGHTK, RIGHTT],
  right: SimpleQueryable[RIGHTK,RIGHTT,RIGHTF]
) extends Queryable[LEFTK, (LEFTT, RIGHTF[RIGHTT]), LEFTF] {
  override def keyNames = left.keyNames
}
 */

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
