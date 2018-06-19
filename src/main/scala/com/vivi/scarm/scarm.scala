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

private[scarm] object dslUtil {
  def alias(name: String, ct: Int): String = name + " AS " + tname(ct)

  def chainDdl(ddl: ConnectionIO[Int]*): ConnectionIO[Int] =
    ddl.tail.fold(ddl.head)( (l,r) => l.flatMap(i => r.map(_+i)))

  def chainDml[T](seq: Seq[T], f: T => ConnectionIO[Int]): ConnectionIO[Int] = {
    val first = f(seq.head)
    seq.tail.foldLeft(first)( (io,e) => io.flatMap(i => f(e).map(_+i)))
  }

  def tname(ct: Int): String = "t" + ct
}

import dslUtil._

/** A database object such as a table or index, created in the RDBMS. */
trait DatabaseObject {
  def name: String
  private[scarm] def tablect: Int = 1
  private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(name, ct))
}


/** A query that takes a K and returns an F[E].  RT is the tuple type
  * of an individual row. */

trait Queryable[K, F[_], E, RT] {

  def keyNames: Seq[String]

  def apply(k: K)(implicit kComp: Composite[K], rtComp: Composite[RT])
      :ConnectionIO[F[E]] = query(k)(kComp, rtComp)

  /* ALAS, this doesn't work for composite K.  Doobie doesn't support it.*/
  def in(k: K*)(implicit kComp: Param[K], rtComp: Composite[RT]): ConnectionIO[Set[E]] = {
    val sql = Fragment.const(keyNames.mkString(","))
    val keys = NonEmptyList.of(k.head, k.tail:_*)
    where(Fragments.in(sql, keys))
  }

  def byTuple[T<:Product,Repr<:HList](t: T)(implicit
    kgen: Generic.Aux[K,Repr],
    tupler: hlist.Tupler.Aux[Repr,T],
    detupler: Generic.Aux[T,Repr],
    kcomp: Composite[K],
    rtcomp: Composite[RT]
  ): ConnectionIO[F[E]] = apply(kgen.from(detupler.to(t)))(kcomp, rtcomp)

  private[scarm] def innerJoinKeyNames: Seq[String] = joinKeyNames
  private[scarm] def joinKeyNames: Seq[String] = keyNames
  private[scarm] def selectList(ct: Int): String
  private[scarm] def tableList(ct: Int): Seq[String]
  private[scarm] def tablect: Int

  private[scarm] def reduceResults(rows: Traversable[RT]): Traversable[E]
  private[scarm] def collectResults[T](reduced: Traversable[T]): F[T]
  private[scarm] def collectResultsToSet[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet


  private[scarm] def whereClause: String = keyNames.map(k =>
    s"${tname(1)}.${k}=?").mkString(" AND ")

  private[scarm] def whereClauseNoAlias: String = keyNames.map(k =>
    s"${k}=?").mkString(" AND ")

  private def selectSql = {
    val tables = tableList(1).mkString(" ")
    s"SELECT ${selectList(1)} FROM ${tables} WHERE "
  }

  lazy val sql: String =  s"${selectSql} ${whereClause}"

  private def runFragment(frag: Fragment, rtComp: Composite[RT]): ConnectionIO[Traversable[E]] =
    frag.query[RT](rtComp).to[List].map(reduceResults(_))

  private[scarm] def doobieQuery(key: K, kComp: Composite[K]) = Fragment(sql,key)(kComp)

  def query(key: K)(implicit kComp: Composite[K], rtComp: Composite[RT]): ConnectionIO[F[E]] =
    runFragment(doobieQuery(key, kComp), rtComp).map(collectResults(_))

  private lazy val selectFrag = Fragment.const(selectSql)

  def where(whereFrag: Fragment)(implicit rtComp: Composite[RT]): ConnectionIO[Set[E]] =
    runFragment(selectFrag ++ whereFrag, rtComp).map(collectResultsToSet(_))


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

