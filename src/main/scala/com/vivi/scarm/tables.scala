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


case class Table[K, E](
  name: String,
  fieldMap: FieldMap[E],
  keyNames: Seq[String],
  autogen: Boolean,
  config: ScarmConfig,
  keyComposite: Composite[K],
  entityComposite: Composite[E],
  primaryKey: PrimaryKey[K,E]
) extends DatabaseObject with Queryable[K,Option,E,E] {

  def autogenField = fieldMap.fields.head
  lazy val autogenFieldName = autogenField.name(config)
  lazy val fieldNames: Seq[String] = fieldMap.names(config)
  lazy val nonKeyFieldNames = fieldNames.filter(!keyNames.contains(_))
  lazy val primaryKeyIndex = UniqueIndex[K,K,E](name+"_pk", this, keyNames)

  override private[scarm] def selectList(ct: Int): String =
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    primaryKeyIndex.collectResults(reduced)

  def create: ConnectionIO[Int] = createWithTypeOverrides(Map())

  def createWithTypeOverrides(overrides: PartialFunction[String,String]): ConnectionIO[Int] = {
    val sql = Table.createSql(this, overrides)
    val create = Fragment(sql, ()).update.run
    if (config.dialect != Postgresql || !autogen) create
    else {
      val sql =  "CREATE SEQUENCE IF NOT EXISTS " + Table.sequenceName(name, autogenFieldName)
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
    if (config.dialect != Postgresql || !autogen) drop
    else {
      val sql = "DROP SEQUENCE " + Table.sequenceName(name, autogenFieldName)
      val fragment = Fragment(sql, ()).update.run
      chainDdl(drop, fragment)
    }
  }

  def dropCascade: ConnectionIO[Int] = {
    val sql = s"DROP TABLE IF EXISTS ${name} CASCADE"
    Fragment(sql, ()).update.run
  }


  def insert[EList<:HList,ETail<:HList,REM](entities: E*)
    (implicit eGeneric: LabelledGeneric.Aux[E,EList],
      remList:  hlist.Drop.Aux[EList,Nat._1,ETail],
      remComposite: Composite[ETail],
      remTupler: hlist.Tupler.Aux[ETail,REM]
  ): ConnectionIO[Int] = {
    if (autogen) {
      chainDml(entities, (e: E) => insertOne(e))
    } else {
      val nel: NonEmptyList[E] = NonEmptyList.of(entities.head, entities.tail:_*)
      Update[E](insertSql)(entityComposite).updateMany(nel)
    }
  }

  private[scarm] def insertOne[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[Int] = insertFragment(entity).run

  def insertReturningKey[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[K] = {
    val frag = insertFragment(entity)
    if (!autogen) frag.run.map(_ => primaryKey(entity))
    else {
      config.dialect match {
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
      doobieQuery(k,keyComposite).query[E](entityComposite).unique
    config.dialect match {
      case Postgresql =>
        dml.withUniqueGeneratedKeys[E](fieldNames:_*)(entityComposite)
      case Hsqldb|Mysql if !autogen => 
        dml.run.flatMap(_ => fetchEntity(primaryKey(entity)))
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

  lazy val scan = TableScan(this)

  def update[KList<:HList,EList<:HList, REMList<:HList, REV,REVList<:HList]
    (entities: E*)(implicit  updater: Updater[K,E]): ConnectionIO[Int] = {
    updater.updateBatch(this, entities:_*)
  }

  private[scarm] lazy val updateSql: String = {
    val updatedCols = nonKeyFieldNames.map(f => s"${f}=?").mkString(",")
    s"UPDATE ${name} SET ${updatedCols} WHERE ${whereClauseNoAlias}"
  }
}


object Table {

  def apply[K,E](name: String)(implicit
    config: ScarmConfig,
    kmap: FieldMap[K],
    fmap: FieldMap[E],
    kcomp: Composite[K],
    ecomp: Composite[E],
    primaryKey: PrimaryKey[K,E]
  ): Table[K,E] = {
    val prefixedKey = kmap.prefix(fmap.firstFieldName)
    val pkeyNames = prefixedKey.names(config)
    Table[K,E](name, fmap, pkeyNames, false, config, kcomp,   ecomp, primaryKey)
  }

  def apply[K,E](name: String, kNames: Seq[String])
    (implicit config: ScarmConfig,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E]
    ): Table[K,E] =
    Table[K,E](name, fmap, kNames, false, config, kcomp,
      ecomp, primaryKey)

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

  private def typeName(table: Table[_,_],
    item: FieldMap.Item,
    overrides: PartialFunction[String,String]
  ) = {
    val nullable = if (item.optional) "" else "not null"
    val columnName = item.name(table.config)
    val dialect = table.config.dialect
    val auto =
      if (!table.autogen || item != table.autogenField) ""
      else autogenModifier(dialect, table.name, columnName)
    val columnType =
      if (overrides.isDefinedAt(columnName)) overrides(columnName)
      else sqlTypeMap(dialect).get(item.tpe.typeSymbol) match {
          case Some(t) => t
          case None =>  throw new RuntimeException(s"Could not find sql type for type ${item.tpe.companion}")
        }
    s"${columnType} ${nullable} ${auto}"
  }

  def createSql[K,E](table: Table[K,E], typeOverrides: PartialFunction[String,String]): String = {
    val columns = table.fieldMap.fields.map(item => 
             item.name(table.config) + " " + typeName(table, item, typeOverrides)
    ).mkString(", ")
    val pkeyColumns = table.keyNames.mkString(",")
    s"CREATE TABLE ${table.name} (${columns}, PRIMARY KEY (${pkeyColumns}))"
  }

  private[scarm] val defaultSqlTypeMap: Map[RSymbol, String] = Map(
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
    typeOf[java.time.LocalTime].typeSymbol -> "TIME",
    typeOf[java.util.UUID].typeSymbol -> "CHAR(36)"
  )

  private[scarm] val sqlTypeMap: Map[SqlDialect, Map[RSymbol, String]] = Map(
    Postgresql -> (defaultSqlTypeMap ++ Map(
      typeOf[String].typeSymbol -> "TEXT",
      typeOf[java.util.UUID].typeSymbol -> "UUID"
    )),
    Mysql -> defaultSqlTypeMap,
    Hsqldb -> defaultSqlTypeMap
  )
}


object Autogen {

  def apply[K,E](name: String)
    (implicit config: ScarmConfig,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E]
    ): Table[K,E] = Table[K,E](name).copy(autogen=true)

  def apply[K,E](name: String, kNames: Seq[String])
    (implicit config: ScarmConfig,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E],
    ): Table[K,E] = Table[K,E](name,kNames).copy(autogen=true)
}

