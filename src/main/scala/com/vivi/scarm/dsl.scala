package com.vivi.scarm

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream
import cats.effect.IO

import scala.util.{Failure,Success,Try}

import scala.language.higherKinds

import cats.Id
import doobie._
import doobie.implicits._


trait Entity[K] {
  def id: K
}

object dslUtil {
  def tname(ct: Int): String = "t" + ct
  def alias(name: String, ct: Int): String = name + " AS " + tname(ct)
}

import dslUtil._

sealed trait DatabaseObject {
  def name: String

  def selectList(ct: Int): String = tname(ct) +".*"
  def tablect: Int = 1
  def tableList(ct: Int): Seq[String] = Seq(alias(name, ct))
}


sealed trait Queryable[K, F[_], E, RT] {

  def innerJoinKeyNames: Seq[String] = joinKeyNames
  def joinKeyNames: Seq[String] = keyNames
  def keyNames: Seq[String]
  def orderBy(ct: Int): String
  def selectList(ct: Int): String
  def tableList(ct: Int): Seq[String]
  def tablect: Int
  def whereClause: String = keyNames.map(k => s"t1.${k}=?").mkString(" AND ")

  def sql: String = {
    val tables = tableList(1).mkString(" ")
    s"SELECT ${selectList(1)} FROM ${tables} WHERE ${whereClause} ORDER BY ${orderBy(1)}" 

  }

  def reduceResults(rows: Traversable[RT]): Traversable[E]
  def collectResults[T](reduced: Traversable[T]): F[T]

  def doobieQuery(key: K)(implicit keyComposite: Composite[K], compositeT: Composite[RT]): Query0[RT] =
    Fragment(sql, key)(keyComposite).query[RT](compositeT)

  def query(key: K)
    (implicit xa: Transactor[IO], keyComposite: Composite[K], compositeT: Composite[RT]): F[E] = {
    val dquery = doobieQuery(key)(keyComposite, compositeT)
    val rows: Seq[RT] = dquery.to[List].transact(xa).unsafeRunSync
    collectResults(reduceResults(rows))
  }

  def join[LK,LF[_]](query: Queryable[LK,LF,K,_])
      :Queryable[LK,LF, (K,F[E]), (K,RT)] =
    Join(query,this)

  def ::[LK,LF[_]](query: Queryable[LK,LF,K,_])
      :Queryable[LK,LF, (K,F[E]), (K,RT)] =
    join(query)

  def nestedJoin[LK,LF[_],X,RT2](query: Queryable[LK,LF,(K,X),RT2])
        :Queryable[LK,LF, (K,X,F[E]), (K,X,RT)] = NestedJoin(query,this)

  def :::[LK,LF[_],X](query: Queryable[LK,LF,(K,X),_])
      :Queryable[LK,LF, (K,X,F[E]), (K,X,RT)] = nestedJoin(query)
}


case class Table[K,E<:Entity[K]](
  override val name: String,
  override val keyNames: Seq[String] = Seq("id")
) extends DatabaseObject with Queryable[K,Id,E,E] {

  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this, { _.id}, keyNames)

  override def orderBy(ct: Int): String =
    keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override def collectResults[T](reduced: Traversable[T]): Id[T] = reduced.head

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
) extends DatabaseObject with Queryable[K,Option,E,E] {

  override def tableList(ct: Int = 0): Seq[String] =
    Seq(alias(s"(${definition})", ct))

  override def orderBy(ct: Int): String =
    keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override def collectResults[T](reduced: Traversable[T]): Option[T] = 
    if (reduced.isEmpty) None else Some(reduced.head)
}


case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E,E] {

  override def tableList(ct: Int=0): Seq[String ]= Seq(alias(table.name, ct))

  override def orderBy(ct: Int): String =
    table.keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override def collectResults[T](reduced: Traversable[T]): Set[T] = reduced.toSet
}


case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  key: E => K,
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E,E] {

  override def tableList(ct: Int=0): Seq[String] = Seq(alias(table.name, ct))

  override def orderBy(ct: Int): String =
    table.keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends DatabaseObject
    with Queryable[FROM, F, TO, TO] {

  def from: Table[FPK,FROM]
  def fromKey: FROM => TPK
  def to: Table[TPK,TO]
  override def keyNames: Seq[String]
  override def joinKeyNames: Seq[String] = to.keyNames

  override def orderBy(ct: Int): String =
    to.keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override lazy val name: String = s"${from.name}_${to.name}_fk"

  override def selectList(ct: Int): String = to.selectList(ct)
  override def tableList(ct: Int): Seq[String] = Seq(alias(to.name, ct))

  lazy val oneToMany = ReverseForeignKey(this)
  lazy val reverse = oneToMany

  lazy val index: Index[TPK,FPK,FROM] = Index(name+"_ix", from, fromKey, keyNames)
}


case class MandatoryForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Id] {
  override def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override def collectResults[T](reduced: Traversable[T]): Id[T] = reduced.head
}


case class OptionalForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Option] {
  override def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override def collectResults[T](reduced: Traversable[T]): Option[T] = 
    if (reduced.isEmpty) None else Some(reduced.head)
}


case class ReverseForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK],F[_]](
  val foreignKey: ForeignKey[TPK,TO,FPK,FROM,F]
) extends Queryable[FROM, Set, TO, TO] {

  override def keyNames: Seq[String] = foreignKey.to.keyNames
  override def joinKeyNames: Seq[String] = foreignKey.keyNames
  override def selectList(ct: Int): String = foreignKey.to.selectList(ct)
  override def tablect: Int = 1

  override def tableList(ct: Int): Seq[String] =
    Seq(alias(foreignKey.from.name, ct))

  override def orderBy(ct: Int): String =
    foreignKey.from.keyNames.map(k => tname(ct) +"."+k).mkString(",")

  override def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override def collectResults[T](reduced: Traversable[T]): Set[T] =  reduced.toSet
}


case class Join[K,LF[_], JK, LRT, RF[_],E, RRT](
  left: Queryable[K,LF,JK,LRT],
  right: Queryable[JK,RF,E,RRT]
) extends Queryable[K,LF,(JK,RF[E]), (JK,RRT)] {

  override def keyNames = left.keyNames
  override def innerJoinKeyNames: Seq[String] = left.joinKeyNames
  override def joinKeyNames = left.joinKeyNames  

  override def orderBy(ct: Int): String =
    left.orderBy(ct)+","+right.orderBy(ct+left.tablect)

  override def reduceResults(rows: Traversable[(JK,RRT)]): Traversable[(JK,RF[E])] = 
    rows.groupBy(_._1).
      mapValues(_.map(_._2)).
      mapValues(s => right.collectResults(right.reduceResults(s)))


  override def collectResults[T](reduced: Traversable[T]): LF[T] =
    left.collectResults(reduced)


  override def selectList(ct: Int): String =
    left.selectList(ct) +","+ right.selectList(ct+1)

  override def tablect: Int = left.tablect + right.tablect

  private def joinCondition(ct: Int): String = {
    val leftt = tname(ct+left.tablect-1)
    val rightt = tname(ct+left.tablect)
    (right.keyNames zip right.joinKeyNames).
      map(p => s"${leftt}.${p._1} = ${rightt}.${p._2}").
      mkString(" AND ")
  }

  override def tableList(ct: Int): Seq[String] = {
    val rct = ct+left.tablect
    val rtables = right.tableList(rct)
    val rhead = s"LEFT OUTER JOIN ${rtables.head} ON ${joinCondition(ct)}"
    (left.tableList(ct) :+ rhead) ++ rtables.tail
  }
}


case class NestedJoin[K,LF[_], LRT,JK,X, RF[_],E,RRT](
  left: Queryable[K,LF,(JK,X),LRT],
  right: Queryable[JK,RF,E,RRT]
) extends Queryable[K,LF,(JK,X,RF[E]), (JK,X,RRT)] { 

  override def keyNames = left.keyNames
  override def innerJoinKeyNames: Seq[String] = left.joinKeyNames
  override def joinKeyNames = left.joinKeyNames

  override def orderBy(ct: Int): String =
    left.orderBy(ct)+","+right.orderBy(ct+left.tablect)

  override def reduceResults(rows: Traversable[(JK,X,RRT)]): Traversable[(JK,X,RF[E])] = 
    rows.groupBy(t => (t._1, t._2)).
      mapValues(_.map(_._3)).
      mapValues(s => right.collectResults(right.reduceResults(s))).
      map(t => (t._1._1, t._1._2, t._2))


  override def collectResults[T](reduced: Traversable[T]): LF[T] =
    left.collectResults(reduced)

  override def selectList(ct: Int): String =
    left.selectList(ct) +","+ right.selectList(ct+left.tablect)

  override def tablect: Int = left.tablect + right.tablect

  private def joinCondition(ct: Int): String = {
    val leftt = tname(ct)
    val rightt = tname(ct+left.tablect)
    (right.keyNames zip right.joinKeyNames).
      map(p => s"${leftt}.${p._1} = ${rightt}.${p._2}").
      mkString(" AND ")
  }

  override def tableList(ct: Int): Seq[String] = {
    val rct = ct+left.tablect
    val rtables = right.tableList(rct)
    val rhead = s"LEFT OUTER JOIN ${rtables.head} ON ${joinCondition(ct)}"
    (left.tableList(ct) :+ rhead) ++ rtables.tail
  }
}
