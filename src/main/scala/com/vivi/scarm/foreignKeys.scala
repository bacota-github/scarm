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

trait ManyToOne[FPK,FROM,TPK,TO,F[_]] extends Queryable[FROM, F, TO, TO] {
  def from: Table[FPK,FROM]
  def to: Table[TPK,TO]
  override private[scarm] def joinKeyNames: Seq[String] = to.keyNames
  override private[scarm] def selectList(ct: Int): String = to.selectList(ct)
  override private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(to.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override def tablect: Int = 1
}

case class OptionalManyToOne[FPK,FROM,TPK,TO](
  from: Table[FPK,FROM],
  to: Table[TPK,TO],
  override val keyNames: Seq[String]
) extends ManyToOne[FPK,FROM,TPK,TO,Option] {
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)
}

case class MandatoryManyToOne[FPK,FROM,TPK,TO](
  from: Table[FPK,FROM],
  to: Table[TPK,TO],
  override val keyNames: Seq[String]
) extends ManyToOne[FPK,FROM,TPK,TO,Id] {
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Id[T] = reduced.head
}

case class OneToMany[FPK,ONE,TPK,MANY](
  one: Table[FPK,ONE],
  many: Table[TPK,MANY],
  override val joinKeyNames: Seq[String]
) extends Queryable[ONE, Set, MANY, MANY] {
  override def keyNames: Seq[String] = one.keyNames
  override private[scarm] def selectList(ct: Int): String = many.selectList(ct)
  override private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(many.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[MANY]): Traversable[MANY] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet
  override def tablect: Int = 1
}



trait ForeignKey[FPK, FROM, TPK, TO] extends DatabaseObject {
  def from: Table[FPK,FROM]
  def to: Table[TPK,TO]
  def keyMapping: Seq[(String,String)]
  override lazy val name: String = s"${from.name}_${to.name}_fk"
  lazy val keyNames = keyMapping.map(_._1)

  def create: ConnectionIO[Int] = {
    val keys = keyNames.mkString(",")
    val pkeyNames = to.keyNames.mkString(",")
    val sql = s"ALTER TABLE ${from.name} ADD FOREIGN KEY (${keys}) REFERENCES ${to.name} (${pkeyNames})"
    Fragment(sql, ()).update.run
  }

  lazy val index = Index[TPK,FPK,FROM](name + "_idx", from, keyNames)
  def fetchBy = index
  lazy val oneToMany: OneToMany[TPK,TO,FPK,FROM] = OneToMany(to,from,keyNames)
}


case class MandatoryForeignKey[FPK, FROM, TPK, TO](
  from: Table[FPK,FROM],
  to: Table[TPK,TO],
  val keyMapping: Seq[(String,String)]
) extends ForeignKey[FPK,FROM,TPK,TO] {
  lazy val manyToOne: MandatoryManyToOne[FPK,FROM,TPK,TO] = MandatoryManyToOne(from,to,keyNames)
}


object MandatoryForeignKey {
  def apply[FPK,FROM,FK,FKRepr,TPK,TO](from: Table[FPK,FROM], to:Table[TPK,TO])
    (implicit foreignKeyIsSubsetOfChildEntity: Subset[FK,FROM],
      foreignKeyStructureMatchesPrimaryKey: EqualStructure[FK,TPK],
      fkmap: FieldMap[FK]
    ): MandatoryForeignKey[FPK,FROM,TPK,TO] = {
    val fknames = fkmap.names(from.config)
    if (to.keyNames.size != fknames.size)
      throw new RuntimeException(s"Foreign key ${fknames.mkString} from ${from.name} to ${to.name} does not match primary key ${to.keyNames.mkString}")
    MandatoryForeignKey(from,to, fknames.zip(to.keyNames))
  }

  def apply[FPK,FROM,FK,FKRepr,TPK,TO]
    (from: Table[FPK,FROM], to:Table[TPK,TO], clazz: Class[FK])
    (implicit foreignKeyIsSubsetOfChildEntity: Subset[FK,FROM],
      foreignKeyStructureMatchesPrimaryKey: EqualStructure[FK,TPK],
      fkmap: FieldMap[FK]
    ): MandatoryForeignKey[FPK,FROM,TPK,TO] =
    apply(from, to)(foreignKeyIsSubsetOfChildEntity,foreignKeyStructureMatchesPrimaryKey,fkmap)
}


case class OptionalForeignKey[FPK, FROM, TPK, TO](
  from: Table[FPK,FROM],
  to: Table[TPK,TO],
  val keyMapping: Seq[(String,String)]
) extends ForeignKey[FPK,FROM,TPK,TO] {
  lazy val manyToOne: OptionalManyToOne[FPK,FROM,TPK,TO] = OptionalManyToOne(from,to,keyNames)
}


object OptionalForeignKey {
  def apply[FPK,FROM,FK,FKRepr,TPK,TO](from: Table[FPK,FROM], to:Table[TPK,TO])
    (implicit foreignKeyIsSubsetOfChildEntity: Subset[FK,FROM],
      foreignKeyStructureMatchesPrimaryKey: SimilarStructure[FK,TPK],
      fkmap: FieldMap[FK]
    ): OptionalForeignKey[FPK,FROM,TPK,TO] = {
    val fknames = fkmap.names(from.config)
    if (to.keyNames.size != fknames.size)
      throw new RuntimeException(s"Foreign key ${fknames.mkString} from ${from.name} to ${to.name} does not match primary key ${to.keyNames.mkString}")
    OptionalForeignKey(from,to, fknames.zip(to.keyNames))
  }

  def apply[FPK,FROM,FK,FKRepr,TPK,TO]
    (from: Table[FPK,FROM], to:Table[TPK,TO], clazz: Class[FK])
    (implicit foreignKeyIsSubsetOfChildEntity: Subset[FK,FROM],
      foreignKeyStructureMatchesPrimaryKey: SimilarStructure[FK,TPK],
      fkmap: FieldMap[FK]
    ): OptionalForeignKey[FPK,FROM,TPK,TO] =
    apply(from, to)(foreignKeyIsSubsetOfChildEntity,foreignKeyStructureMatchesPrimaryKey,fkmap)
}
