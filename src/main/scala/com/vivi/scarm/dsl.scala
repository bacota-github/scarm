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


sealed trait Joinable[K, T, F[_]]

sealed trait JoinableQueryable[K, T, F[_]]
    extends Queryable[K, T, F] with Joinable[K, T, F] {
}


trait Entity[K] {
  def id: K
}

trait Table[K,E<:Entity[K]]
    extends DatabaseObject
    with JoinableQueryable[K,E,Id] {

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
  id: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with JoinableQueryable[K,E,F] {
  override def create: Update0 = null
  override def drop: Update0 = null 
}

case class Index[PK,K,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  id: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with JoinableQueryable[K,E,Set] {
  override def create: Update0 = null
  override def drop: Update0 = null
}

case class UniqueIndex[PK,K,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  id: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with JoinableQueryable[K,E,Option] {
  override def create: Update0 = null
  override def drop: Update0 = null
}


case class ForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  from: Table[FPK,FROM],
  fromKey: FROM => TPK,
  to: Table[TPK,TO],
  val keyNames: Seq[String]
) extends DatabaseObject {
  override def create: Update0 = null
  override def drop: Update0 = null
  override lazy val name: String = s"${from.name}_${to.name}_fk"
  lazy val manyToOne = ManyToOne(to, fromKey, to.primaryKey.id)
  lazy val oneToMany = OneToMany[TO,TPK,FROM](index, fromKey, to.primaryKey.id)
  lazy val index = Index(name+"_ix", from, fromKey, keyNames)
}

case class OptionalForeignKey[FPK,MANY<:Entity[FPK],TPK,TO<:Entity[TPK]](
  from: Table[FPK,MANY],
  fromKey: MANY => TPK,
  to: Table[TPK,TO],
  val keyNames: Seq[String]
) extends DatabaseObject {
  override def create: Update0 = null
  override def drop: Update0 = null
  override lazy val name = s"${from.name}_${to.name}_fk"
  lazy val manyToOne = OptionalManyToOne(to.primaryKey, fromKey, to.primaryKey.id)
  lazy val oneToMany = OneToMany[TO,TPK,MANY](index, fromKey, to.primaryKey.id)
  lazy val index = Index(name+"_ix", from, fromKey, keyNames)
}

case class ManyToOne[MANY,PK,ONE](
  one: Queryable[PK,ONE,Id],
  manyKey: MANY => PK,
  oneKey: ONE => PK
) extends JoinableQueryable[MANY,ONE,Id]{

  def join[K,F[_]](left: Joinable[K,MANY,F]) =
    Join[K,MANY,F,PK,ONE,Id](left,manyKey,one)

  def ::[K,F[_]](left: Joinable[K,MANY,F]) = join(left)

  def +:[K,F[_]](left: Joinable[K,MANY,F]) =
    ManyToOneJoin[K,F,PK,MANY,ONE](left, this)
}

case class OptionalManyToOne[MANY,PK,ONE](
  one: Queryable[PK,ONE,Option],
  manyKey: MANY => PK,
  oneKey: ONE => PK
) extends JoinableQueryable[MANY,ONE,Option]{

  def join[K,F[_]](left: Joinable[K,MANY,F]) =
    Join[K,MANY,F,PK,ONE,Option](left,manyKey,one)

  def ::[K,F[_]](left: Joinable[K,MANY,F]) = join(left)

  def +:[K,F[_]](left: Joinable[K,MANY,F]) =  OptionalManyToOneJoin(left, this)
}

case class OneToMany[ONE,PK,MANY](
  many: Queryable[PK,MANY,Set],
  manyKey: MANY => PK,
  oneKey: ONE => PK
) extends JoinableQueryable[ONE,MANY,Set] {

  def join[K,F[_]](left: Joinable[K,ONE,F])  =
    Join[K,ONE,F,PK,MANY,Set](left,oneKey,many)

  def ::[K,F[_]](left: Joinable[K,ONE,F]) = join(left)

  def +:[K,F[_]](left: Joinable[K,ONE,F]) = join(left)
}

case class Join[LEFTK,LEFTT,LEFTF[_], RIGHTK, RIGHTT, RIGHTF[_]](
  left: Joinable[LEFTK,LEFTT,LEFTF],
  key: LEFTT => RIGHTK,
  right: Queryable[RIGHTK,RIGHTT,RIGHTF]
) extends Joinable[LEFTK, (LEFTT, RIGHTF[RIGHTT]), LEFTF] 
    with Queryable[LEFTK, (LEFTT, RIGHTF[RIGHTT]), LEFTF] {

  override def query(key: LEFTK) = Stream()

  def identity[T](t: T) = t
  def join[K,F[_]](left: Joinable[K,LEFTK,F]) =
    Join[K,LEFTK,F, LEFTK,(LEFTT,RIGHTF[RIGHTT]), LEFTF](left,identity(_),this)
  def ::[K,F[_]](left: Joinable[K,LEFTK,F]) = join(left)
}

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


