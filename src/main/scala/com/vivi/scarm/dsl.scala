package com.vivi.scarm

import scala.language.implicitConversions

import com.vivi.scarm._

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream
import cats.effect.IO

import scala.util.{Failure,Success,Try}
import scala.reflect.runtime.universe.{Type,typeOf,Symbol=>RSymbol}
import scala.language.higherKinds

import doobie._
import doobie.implicits._
import shapeless.{ Generic, HList, LabelledGeneric, Lazy, Lens,MkFieldLens, Nat, Witness, lens }
import shapeless.ops.hlist
import shapeless.ops.hlist.{Length, Prepend}

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

trait Entity[+K] {
  def id: K
}


/** A database object such as a table or index, created in the RDBMS. */
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

  def apply(k: K)(implicit kComp: Composite[K], rtComp: Composite[RT])
      :ConnectionIO[F[E]] = query(k)(kComp, rtComp)


  private[scarm] def innerJoinKeyNames: Seq[String] = joinKeyNames
  private[scarm] def joinKeyNames: Seq[String] = keyNames
  private[scarm] def selectList(ct: Int): String
  private[scarm] def tableList(ct: Int): Seq[String]
  private[scarm] def tablect: Int

  private[scarm] def whereClause: String = keyNames.map(k =>
    s"${tname(1)}.${k}=?").mkString(" AND ")

  private[scarm] def whereClauseNoAlias: String = keyNames.map(k =>
    s"${k}=?").mkString(" AND ")

  lazy val sql: String = {
    val tables = tableList(1).mkString(" ")
    s"SELECT ${selectList(1)} FROM ${tables} WHERE ${whereClause}"

  }

  private[scarm] def reduceResults(rows: Traversable[RT]): Traversable[E]
  private[scarm] def collectResults[T](reduced: Traversable[T]): F[T]

  def doobieQuery(key: K)(implicit kComp: Composite[K], rtComp: Composite[RT]): Query0[RT] =
    Fragment(sql, key)(kComp).query[RT](rtComp)

  def query(key: K)
    (implicit kComp: Composite[K], rtComp: Composite[RT]): ConnectionIO[F[E]] = {
    val dquery = doobieQuery(key)(kComp, rtComp)
    dquery.to[List].map(reduceResults(_)).map(collectResults(_))
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


case class Table[K, E<:Entity[K]](
  name: String,
  id: E=>K,
  fieldMap: FieldMap[E],
  keyNames: Seq[String],
  autogen: Boolean,
  dialect: SqlDialect,
  keyComposite: Composite[K],
  entityComposite: Composite[E]
) extends DatabaseObject with Queryable[K,Option,E,E] {

  lazy val autogenField = fieldMap.fields.head.name
  lazy val fieldNames: Seq[String] = fieldMap.names
  lazy val nonKeyFieldNames = fieldNames.filter(!keyNames.contains(_))

  lazy val primaryKey = UniqueIndex[K,K,E](name+"_pk", this,
    (e:E) => Some(e.id), keyNames)

  override private[scarm] def selectList(ct: Int): String =
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    primaryKey.collectResults(reduced)


  def create(
    fieldOverrides: Map[String, String] = Map(),
    typeOverrides: Map[Type, String] = Map()
  ): ConnectionIO[Int] = {
    val sql = Table.createSql(dialect, this,fieldOverrides,typeOverrides)
    val create = Fragment(sql, ()).update.run
    if (dialect != Postgresql || !autogen) create
    else {
      val sql =  "CREATE SEQUENCE IF NOT EXISTS " + Table.sequenceName(name, autogenField)
      val fragment = Fragment(sql, ()).update.run
      chainDdl(fragment, create)
    }
  }

  def delete(keys: K*): ConnectionIO[Int] = 
    chainDml(keys, deleteOne)

  private def deleteOne(key: K): ConnectionIO[Int] = {
    val sql =  s"DELETE FROM ${name} WHERE ${whereClauseNoAlias}"
    Fragment(sql, key)(keyComposite).update.run
  }

  def drop: ConnectionIO[Int] = {
    val sql = s"DROP TABLE IF EXISTS ${name}"
    val drop = Fragment(sql, ()).update.run
    if (dialect != Postgresql || !autogen) drop
    else {
      val sql = "DROP SEQUENCE " + Table.sequenceName(name, autogenField)
      val fragment = Fragment(sql, ()).update.run
      chainDdl(drop, fragment)
    }
  }

  def dropCascade: ConnectionIO[Int] = {
    val sql = s"DROP TABLE IF EXISTS ${name} CASCADE"
    Fragment(sql, ()).update.run
  }

  def insert[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[Int] = insertFragment(entity).run


  def insertBatch[EList<:HList,REMList<:HList,REM](entities: E*)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[Int] = chainDml(entities, (e: E) => insert(e))

  def insertReturningKey[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[K] = {
    val frag = insertFragment(entity)
    if (!autogen) frag.run.map(_ => entity.id)
    else {
      dialect match {
        case Hsqldb|Postgresql => 
          frag.withUniqueGeneratedKeys[K](keyNames.head)(keyComposite)
        case Mysql => {
          val select = sql"SELECT LAST_INSERT_ID()".query[K](keyComposite)
          frag.run.flatMap(_ => select.unique)
        }
      }
    }
  }


  def insertBatchReturningKeys[EList<:HList,REMList<:HList,REM](entities: E*)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[Seq[K]] = {
    val first = insertReturningKey(entities.head).map(Seq(_))
    entities.tail.foldLeft(first)( (io,e) =>
      io.flatMap(seq => insertReturningKey(e).map(k => seq :+ k))
    )
  }
  

  def insertReturning[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[E] = {
    val dml = insertFragment(entity)
    def fetchEntity(k: K) =
      this.doobieQuery(k)(keyComposite, entityComposite).unique
    dialect match {
      case Postgresql =>
        dml.withUniqueGeneratedKeys[E](fieldNames:_*)(entityComposite)
      case Hsqldb|Mysql if !autogen => 
        dml.run.flatMap(_ => fetchEntity(entity.id))
      case Hsqldb if autogen =>
        dml.withUniqueGeneratedKeys[K](keyNames.head)(keyComposite).
          flatMap(k => fetchEntity(k))
      case Mysql if autogen => {
        val fetchId = sql"SELECT LAST_INSERT_ID()".query[K](keyComposite).unique
        for {
          _ <- dml.run
          k <- fetchId
          e <- fetchEntity(k)
        } yield e
      }
    }
  }

  def insertBatchReturning[EList<:HList,REMList<:HList,REM](entities: E*)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[Seq[E]] = {
    val first = insertReturning(entities.head).map(Seq(_))
    entities.tail.foldLeft(first)( (io,e) =>
      io.flatMap(seq => insertReturning(e).map(e => seq :+ e))
    )
  }


  private def insertFragment[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ) =
    if (!autogen) Fragment(insertSql, entity)(entityComposite).update
    else {
      val truncated: REMList = remList(eGeneric.to(entity))
      Fragment(insertSqlWithAutogen, truncated)(remComposite).update
    }


  private[scarm] lazy val insertSql: String = {
    val names = fieldNames.mkString(",")
    val values = List.fill(entityComposite.length)("?").mkString(",")
    s"INSERT INTO ${name} (${names}) values (${values})"
  }

  private[scarm] lazy val insertSqlWithAutogen: String = {
    val names = nonKeyFieldNames.mkString(",")
    val values = List.fill(nonKeyFieldNames.length)("?").mkString(",")
    s"INSERT INTO ${name} (${names}) values (${values})"
  }

  def save(enties: E*): Try[Unit] = ???

  lazy val scan = TableScan(this)

  //Primary key columns must be the first columns
  def update[KList<:HList,EList<:HList, REMList<:HList, REV,REVList<:HList]
    (entities: E*)(implicit
      kGeneric: LabelledGeneric.Aux[K,KList],
      eGeneric: LabelledGeneric.Aux[E,EList],
      remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
      revList: hlist.Prepend.Aux[REMList,KList,REVList],
      revComposite: Composite[REVList],
      revTupler: hlist.Tupler.Aux[REVList, REV]
  ): ConnectionIO[Int] = {
    def f(e: E) = updateOne(e,kGeneric, eGeneric, remList, revList,
      revComposite, revTupler)
    val first = f(entities.head)
    if (entities.tail.size == 0) first
    else entities.tail.foldLeft(first)( (io,e) =>
        io.flatMap(i => f(e).map(_+i))
    )
  }

  private def updateOne[KList<:HList,EList<:HList, REMList<:HList, REV,REVList<:HList](entity: E,
    kGeneric: LabelledGeneric.Aux[K,KList],
    eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    revList: hlist.Prepend.Aux[REMList,KList,REVList],
    revComposite: Composite[REVList],
    revTupler: hlist.Tupler.Aux[REVList, REV]
  ): ConnectionIO[Int] = {
    val key: KList = kGeneric.to(entity.id)
    val remainder: REMList = remList(eGeneric.to(entity))
    val reversed: REVList = revList(remainder, key)
    Fragment(updateSql, reversed)(revComposite).update.run
  }

  private lazy val updateSql: String = {
    val updatedCols = nonKeyFieldNames.map(f => s"${f}=?").mkString(",")
    s"UPDATE ${name} SET ${updatedCols} WHERE ${whereClauseNoAlias}"
  }

}


object Table {

  private def keyNames[K](kmap: FieldMap[K]): Seq[String] = {
    val names = kmap.names
    if (names != Seq("")) names else Seq("id")
  }


  def apply[K,E<:Entity[K]](name: String)
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] =
    Table[K,E](name, (e:E) => e.id, fmap, keyNames(kmap), false, dialect, kcomp, ecomp)

  def apply[K,E<:Entity[K]](name: String, kNames: Seq[String])
    (implicit dialect: SqlDialect,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] =
    Table[K,E](name, (e:E) => e.id, fmap, kNames, false, dialect, kcomp, ecomp)

  def apply[K,E<:Entity[K]](name: String, key: Witness.Aux[K])
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      mkFieldLens: MkFieldLens.Aux[E,K,K],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] = {
    val keyLens: Lens[E,K] = lens[E] >> key
    Table(name, keyLens.get(_), fmap, keyNames(kmap), false, dialect, kcomp, ecomp)
  }

  private def sequenceName(tableName: String, fieldName: String) =
    s"${tableName}_${fieldName}_sequence"

  private def autogenModifier(dialect: SqlDialect, tableName: String, fieldName: String) =
    dialect match {
      case Hsqldb => "IDENTITY"
      case Mysql => "AUTO_INCREMENT"
      case Postgresql => {
        val name = sequenceName(tableName, fieldName)
        s"default nextval('${name}')"
      }
    }

  private def typeName(dialect: SqlDialect, table: Table[_,_],
    item: FieldMap.Item, overrides: Map[Type,String]) =  {
    val nullable = if (item.optional) "" else "not null"
    val auto =
      if (!table.autogen || item.name != table.autogenField) ""
      else autogenModifier(dialect, table.name, item.name)
    overrides.getOrElse(item.tpe, sqlTypeMap.get(item.tpe.typeSymbol)) match {
      case None => throw new RuntimeException(s"Could not find sql type for type ${item.tpe.companion}")
      case Some(typeString) => s"${typeString} ${nullable} ${auto}"
    }
  }

  def createSql[K,E<:Entity[K]](
    dialect: SqlDialect,
    table: Table[K,E],
    fieldOverrides: Map[String, String] = Map(),
    typeOverrides: Map[Type, String] = Map()
  ): String = {
    val columns = table.fieldNames.map(f => 
      fieldOverrides.get(f) match {
        case Some(typeString) => f+ " " + typeString
        case None => table.fieldMap.mapping.get(f) match {
          case None => throw new RuntimeException(s"Could not find type for ${f}")
          case Some(item: FieldMap.Item) =>
             f + " " + typeName(dialect, table, item, typeOverrides)
        }
      }).mkString(", ")
    val pkeyColumns = table.keyNames.mkString(",")
    s"CREATE TABLE ${table.name} (${columns}, PRIMARY KEY (${pkeyColumns}))"
  }

  private[scarm] val sqlTypeMap: Map[RSymbol, String] = Map(
    typeOf[Char].typeSymbol -> "CHAR(1)",
    typeOf[String].typeSymbol -> "VARCHAR(255)",
    typeOf[Boolean].typeSymbol -> "BOOLEAN",
    typeOf[Short].typeSymbol -> "SMALLINT",
    typeOf[Int].typeSymbol -> "INT",
    typeOf[Long].typeSymbol -> "BIGINT",
    typeOf[Float].typeSymbol -> "FLOAT",
    typeOf[Double].typeSymbol -> "DOUBLE PRECISION",
    typeOf[java.time.Instant].typeSymbol -> "TIMESTAMP",
    typeOf[java.time.LocalDate].typeSymbol -> "DATE", 
    typeOf[java.time.LocalDateTime].typeSymbol -> "TIMESTAMP",
    typeOf[java.time.LocalTime].typeSymbol -> "TIME"
  )
}


object AutogenTable {

  def apply[K,E<:Entity[K]](name: String)
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] = Table[K,E](name).copy(autogen=true)

  def apply[K,E<:Entity[K]](name: String, kNames: Seq[String])
    (implicit dialect: SqlDialect,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] = Table[K,E](name,kNames).copy(autogen=true)

  def apply[K,E<:Entity[K]](name: String, key: Witness.Aux[K])
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      mkFieldLens: MkFieldLens.Aux[E,K,K],
      kcomp: Composite[K],
      ecomp: Composite[E]
    ): Table[K,E] = Table[K,E](name,key).copy(autogen=true)
}


case class TableScan[K,E<:Entity[K]](table: Table[K,E])
    extends Queryable[Unit, Set, E, E] {
  def keyNames = Seq()
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int): Seq[String] = table.tableList(ct)
  override private[scarm] def tablect: Int = 1
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] =
    table.reduceResults(rows)
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet

  override lazy val sql: String = s"SELECT ${selectList(1)} FROM ${tableList(1).head}"
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

object View {
  def apply[K,E](name: String, definition: String)
    (implicit fieldList: FieldList[E], keyList: FieldList[K]): View[K,E] = 
    View(name, definition, fieldList.names, keyList.names)
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


object Index {
  def apply[K,PK,E<:Entity[PK]](name: String, table: Table[PK,E], key: E=>Option[K])
    (implicit keyList: FieldList[K]): Index[K,PK,E] = 
    Index(name, table, key, keyList.names)
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


object UniqueIndex {
  def apply[K,PK,E<:Entity[PK]](name: String, table: Table[PK,E], key: E=>Option[K])
    (implicit keyList: FieldList[K]): UniqueIndex[K,PK,E] = 
    UniqueIndex(name, table, key, keyList.names)
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
) extends ForeignKey[FPK,FROM,TPK,TO,Option] {
  override def fromKey = (f: FROM) => Some(indexKey(f))
  override private[scarm] def reduceResults(rows: Traversable[TO]): Traversable[TO] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    to.collectResults(reduced)
}


object MandatoryForeignKey {
  def apply[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
    from: Table[FPK,FROM], indexKey: FROM=>TPK, to:Table[TPK,TO]
  )(implicit keyList: FieldList[FROM]): MandatoryForeignKey[FPK,FROM,TPK,TO] =
    MandatoryForeignKey(from, indexKey, to, keyList.names)
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

object OptionalForeignKey {
  def apply[FPK,FROM<:Entity[FPK],TPK,TO<:Entity[TPK]](
    from: Table[FPK,FROM], indexKey: FROM=>Option[TPK], to:Table[TPK,TO]
  )(implicit keyList: FieldList[FROM]): OptionalForeignKey[FPK,FROM,TPK,TO] =
    OptionalForeignKey(from, indexKey, to, keyList.names)
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
