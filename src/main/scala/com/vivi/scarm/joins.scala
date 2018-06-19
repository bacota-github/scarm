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


case class Join[K,LF[_], JK, LRT, RF[_],E, RRT](
  left: Queryable[K,LF,JK,LRT],
  right: Queryable[JK,RF,E,RRT]
) extends Queryable[K,LF,(JK,RF[E]), (JK,Option[RRT])] {

  override def keyNames = left.keyNames
  override private[scarm] def innerJoinKeyNames: Seq[String] = left.joinKeyNames
  override private[scarm] def joinKeyNames = left.joinKeyNames  
  override private[scarm] def reduceResults(rows: Traversable[(JK,Option[RRT])]): Traversable[(JK,RF[E])] =  {
    rows.groupBy(_._1).
      mapValues(_.map(_._2)).
      mapValues(_.collect { case Some(s) => s }).
      mapValues(s =>  right.collectResults(right.reduceResults(s)))
  }

  override private[scarm] def collectResults[T](reduced: Traversable[T]): LF[T] =
    left.collectResults(reduced)

  override private[scarm] def collectResultsToSet[T](reduced: Traversable[T]): Set[T] =
    left.collectResultsToSet(reduced)

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

  override private[scarm] def collectResultsToSet[T](reduced: Traversable[T]): Set[T] =
    left.collectResultsToSet(reduced)

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
