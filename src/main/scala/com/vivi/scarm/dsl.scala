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


private[scarm] object dslUtil {
  def tname(ct: Int): String = "t" + ct
  def alias(name: String, ct: Int): String = name + " AS " + tname(ct)
}

import dslUtil._

/** An object stored in a table.  Id is the primary key value. The
  * entity can be described as a tuple (id,data) */

trait Entity[K] {
  def id: K

  private[scarm] def deleteSQL[E<:Entity[K]](table: Table[K,E]): String =
    s"DELETE FROM ${table.name} AS ${tname(1)} WHERE ${table.whereClause}"

  private[scarm] def insertSQL[E<:Entity[K]](table: Table[K,E])
    (implicit composite: Composite[E]): String = {
    val values = List.fill(composite.length)("?").mkString(",")
    s"INSERT INTO ${table.name} values (${values})"
  }

  /*
  private[scarm] def updateSQL[E<:Entity[K]](table: Table[K,E])
    (implicit composite: Composite[E]): String = {
    val values = List.fill(composite.length)("?").mkString(",")
    s"UPDATE ${table.name} SET
  }
   */
}

/** An database object such as a table or index, created in the RDBMS. */
sealed trait DatabaseObject {
  def name: String
  def fieldNames: Seq[String]
  private[scarm] def tablect: Int = 1
  private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(name, ct))
}


/** A query that takes a K and returns an F[E].  RT is the tuple type
  * of an individual row. */

sealed trait Queryable[K, F[_], E, RT] {

  def keyNames: Seq[String]

  private[scarm] def innerJoinKeyNames: Seq[String] = joinKeyNames
  private[scarm] def joinKeyNames: Seq[String] = keyNames
  private[scarm] def selectList(ct: Int): String
  private[scarm] def tableList(ct: Int): Seq[String]
  private[scarm] def tablect: Int
  private[scarm] def whereClause: String = keyNames.map(k =>
    s"${tname(1)}.${k}=?").mkString(" AND "
  )

  lazy val sql: String = {
    val tables = tableList(1).mkString(" ")
    s"SELECT ${selectList(1)} FROM ${tables} WHERE ${whereClause}"

  }

  private[scarm] def reduceResults(rows: Traversable[RT]): Traversable[E]
  private[scarm] def collectResults[T](reduced: Traversable[T]): F[T]

  def doobieQuery(key: K)(implicit keyComposite: Composite[K], compositeT: Composite[RT]): Query0[RT] =
    Fragment(sql, key)(keyComposite).query[RT](compositeT)

  def query(key: K)
    (implicit xa: Transactor[IO], keyComposite: Composite[K], compositeT: Composite[RT]): F[E] = {
    val dquery = doobieQuery(key)(keyComposite, compositeT)
    val rows: Seq[RT] = dquery.to[List].transact(xa).unsafeRunSync
    collectResults(reduceResults(rows))
  }

  def join[LK,LF[_]](query: Queryable[LK,LF,K,_])
      :Queryable[LK,LF, (K,F[E]), (K,Option[RT])] =
    Join(query,this)

  def ::[LK,LF[_]](query: Queryable[LK,LF,K,_])
      :Queryable[LK,LF, (K,F[E]), (K,Option[RT])] =
    join(query)

  def nestedJoin[LK,LF[_],X,RT2](query: Queryable[LK,LF,(K,X),RT2])
        :Queryable[LK,LF, (K,X,F[E]), (K,X,Option[RT])] = NestedJoin(query,this)

  def :::[LK,LF[_],X](query: Queryable[LK,LF,(K,X),_])
      :Queryable[LK,LF, (K,X,F[E]), (K,X,Option[RT])] = nestedJoin(query)
}


case class Table[K,E<:Entity[K]](
  override val name: String,
  override val fieldNames: Seq[String],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Id,E,E] {

  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this,
    (e:E) => Some(e.id), keyNames)

  override private[scarm] def selectList(ct: Int): String = 
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Id[T] = reduced.head

  private def doDelete(key: K)
    (implicit xa: Transactor[IO], composite: Composite[K]): Unit = {
    val sql =  s"DELETE FROM ${name} AS ${tname(1)} WHERE ${whereClause}"
    Fragment(sql, key)(composite).update.run.transact(xa).unsafeRunSync
  }

  def delete(keys: K*)
    (implicit xa: Transactor[IO], composite: Composite[K]): Unit =
    //TODO: Batch delete
    keys.foreach(doDelete(_)(xa, composite))

  private def doInsert(entity: E)
    (implicit xa: Transactor[IO], composite: Composite[E]): Unit = {
    val sql = entity.insertSQL(this)(composite)
    Fragment(sql, entity)(composite).update.run.transact(xa).unsafeRunSync
  }

  def insert(entities: E*)
    (implicit xa: Transactor[IO], composite: Composite[E]): Unit =
    //TODO: Batch insert
    entities.foreach(doInsert(_)(xa, composite))


  def insertReturning(e: E*): Try[K] = ???

  def save(enties: E*): Try[Unit] = ???
  def update(enties: E*): Try[Unit] = ???
}


object Table {
  def apply[K,E<:Entity[K]](name: String)
    (implicit fieldLister: FieldLister[E], keyLister: FieldLister[K]): Table[K,E] = 
    Table(name, fieldLister.names, keyLister.names)
}

case class View[K,E](
  override val name: String,
  val definition: String,
  override val keyNames: Seq[String],
  override val fieldNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E,E] {

  override private[scarm] def selectList(ct: Int): String = 
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def tableList(ct: Int = 0): Seq[String] =
    Seq(alias(s"(${definition})", ct))

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows

  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)
}


case class Index[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  key: E => Option[K],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E,E] {
  override val fieldNames = table.fieldNames
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String ]= Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] = reduced.toSet
}


case class UniqueIndex[K,PK,E<:Entity[PK]](
  override val name: String,
  table: Table[PK,E],
  key: E => Option[K],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E,E] {
  override val fieldNames = table.fieldNames
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String] = Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)
}


sealed trait ForeignKey[FPK, FROM<:Entity[FPK], TPK, TO<:Entity[TPK], F[_]]
    extends DatabaseObject
    with Queryable[FROM, F, TO, TO] {

  def from: Table[FPK,FROM]
  def fromKey: FROM => Option[TPK]
  def to: Table[TPK,TO]

  override val fieldNames = to.fieldNames
  override def keyNames: Seq[String]
  override private[scarm] def joinKeyNames: Seq[String] = to.keyNames
  override lazy val name: String = s"${from.name}_${to.name}_fk"
  override private[scarm] def selectList(ct: Int): String = to.selectList(ct)
  override private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(to.name, ct))

  lazy val oneToMany = ReverseForeignKey(this)
  lazy val reverse = oneToMany

  lazy val index: Index[TPK,FPK,FROM] = Index(name+"_ix", from, fromKey, keyNames)
}


case class MandatoryForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  val indexKey: FROM => TPK,
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Id] {
  override def fromKey = (f: FROM) => Some(indexKey(f))
  override private[scarm] def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Id[T] = reduced.head
}


case class OptionalForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
  override val from: Table[FPK,FROM],
  override val fromKey: FROM => Option[TPK],
  override val to: Table[TPK,TO],
  override val  keyNames: Seq[String]
) extends ForeignKey[FPK,FROM,TPK,TO,Option] {
  override private[scarm] def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] = 
    if (reduced.isEmpty) None else Some(reduced.head)
}


case class ReverseForeignKey[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK],F[_]](
  val foreignKey: ForeignKey[TPK,TO,FPK,FROM,F]
) extends Queryable[FROM, Set, TO, TO] {

  override val keyNames: Seq[String] = foreignKey.to.keyNames
  override def joinKeyNames: Seq[String] = foreignKey.keyNames
  override def selectList(ct: Int): String = foreignKey.to.selectList(ct)
  override def tablect: Int = 1
  override def tableList(ct: Int): Seq[String] =
    Seq(alias(foreignKey.from.name, ct))
  override def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override def collectResults[T](reduced: Traversable[T]): Set[T] =  reduced.toSet
}


case class Join[K,LF[_], JK, LRT, RF[_],E, RRT](
  left: Queryable[K,LF,JK,LRT],
  right: Queryable[JK,RF,E,RRT]
) extends Queryable[K,LF,(JK,RF[E]), (JK,Option[RRT])] {

  override def keyNames = left.keyNames
  override private[scarm] def innerJoinKeyNames: Seq[String] = left.joinKeyNames
  override private[scarm] def joinKeyNames = left.joinKeyNames  
  override private[scarm] def reduceResults(rows: Traversable[(JK,Option[RRT])]): Traversable[(JK,RF[E])] =  {
    println("Reducing " + rows.toString())
    rows.groupBy(_._1).
      mapValues(_.map(_._2)).
      mapValues(_.collect { case Some(s) => s }).
      mapValues(s =>  right.collectResults(right.reduceResults(s)))
  }

  override private[scarm] def collectResults[T](reduced: Traversable[T]): LF[T] =
    left.collectResults(reduced)

  override private[scarm] def selectList(ct: Int): String =
    left.selectList(ct) +","+ right.selectList(ct+1)

  override private[scarm] def tablect: Int = left.tablect + right.tablect

  private[scarm] def joinCondition(ct: Int): String = {
    val leftt = tname(ct+left.tablect-1)
    val rightt = tname(ct+left.tablect)
    (right.keyNames zip right.joinKeyNames).
      map(p => s"${leftt}.${p._1} = ${rightt}.${p._2}").
      mkString(" AND ")
  }

  override private[scarm] def tableList(ct: Int): Seq[String] = {
    val rct = ct+left.tablect
    val rtables = right.tableList(rct)
    val rhead = s"LEFT OUTER JOIN ${rtables.head} ON ${joinCondition(ct)}"
    (left.tableList(ct) :+ rhead) ++ rtables.tail
  }
}


case class NestedJoin[K,LF[_], LRT,JK,X, RF[_],E,RRT](
  left: Queryable[K,LF,(JK,X),LRT],
  right: Queryable[JK,RF,E,RRT]
) extends Queryable[K,LF,(JK,X,RF[E]), (JK,X,Option[RRT])] { 

  override def keyNames = left.keyNames
  override private[scarm] def innerJoinKeyNames: Seq[String] = left.joinKeyNames
  override private[scarm] def joinKeyNames = left.joinKeyNames

  override private[scarm] def reduceResults(rows: Traversable[(JK,X,Option[RRT])]): Traversable[(JK,X,RF[E])] = 
    rows.groupBy(t => (t._1, t._2)).
      mapValues(_.map(_._3)).
      mapValues(_.collect { case Some(s) => s }).
      mapValues(s => right.collectResults(right.reduceResults(s))).
      map(t => (t._1._1, t._1._2, t._2))

  override private[scarm] def collectResults[T](reduced: Traversable[T]): LF[T] =
    left.collectResults(reduced)

  override private[scarm] def selectList(ct: Int): String =
    left.selectList(ct) +","+ right.selectList(ct+left.tablect)

  override private[scarm] def tablect: Int = left.tablect + right.tablect

  private[scarm] def joinCondition(ct: Int): String = {
    val leftt = tname(ct)
    val rightt = tname(ct+left.tablect)
    (right.keyNames zip right.joinKeyNames).
      map(p => s"${leftt}.${p._1} = ${rightt}.${p._2}").
      mkString(" AND ")
  }

  override private[scarm] def tableList(ct: Int): Seq[String] = {
    val rct = ct+left.tablect
    val rtables = right.tableList(rct)
    val rhead = s"LEFT OUTER JOIN ${rtables.head} ON ${joinCondition(ct)}"
    (left.tableList(ct) :+ rhead) ++ rtables.tail
  }
}
