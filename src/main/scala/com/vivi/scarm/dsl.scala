package com.vivi.scarm

import com.vivi.scarm._

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream
import cats.effect.IO
import cats.Id
import cats.data.NonEmptyList

import scala.reflect.runtime.universe.{Type,TypeTag,typeOf,Symbol=>RSymbol}
import scala.language.higherKinds

import doobie._
import doobie.implicits._
import shapeless.ops.record.Keys
import shapeless.{ Generic, HList, LabelledGeneric, Lazy, Lens,MkFieldLens, Nat, Witness, lens }
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
sealed trait DatabaseObject {
  def name: String
  private[scarm] def tablect: Int = 1
  private[scarm] def tableList(ct: Int): Seq[String] = Seq(alias(name, ct))
}


/** A query that takes a K and returns an F[E].  RT is the tuple type
  * of an individual row. */

sealed trait Queryable[K, F[_], E, RT] {

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


case class Table[K, E](
  name: String,
  fieldMap: FieldMap[E],
  keyNames: Seq[String],
  autogen: Boolean,
  dialect: SqlDialect,
  keyComposite: Composite[K],
  entityComposite: Composite[E],
  primaryKey: PrimaryKey[K,E]
) extends DatabaseObject with Queryable[K,Option,E,E] {

  lazy val autogenField = fieldMap.fields.head.name
  lazy val fieldNames: Seq[String] = fieldMap.names
  lazy val nonKeyFieldNames = fieldNames.filter(!keyNames.contains(_))
  lazy val primaryKeyIndex = UniqueIndex[K,K,E](name+"_pk", this, keyNames)

  override private[scarm] def selectList(ct: Int): String =
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    primaryKeyIndex.collectResults(reduced)

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


  def insertBatch[EList<:HList,ETail<:HList,REM](entities: E*)
    (implicit eGeneric: LabelledGeneric.Aux[E,EList],
      remList:  hlist.Drop.Aux[EList,Nat._1,ETail],
      remComposite: Composite[ETail],
      remTupler: hlist.Tupler.Aux[ETail,REM]
  ): ConnectionIO[Int] = {
    if (autogen) {
      chainDml(entities, (e: E) => insert(e))
    } else {
      val nel: NonEmptyList[E] = NonEmptyList.of(entities.head, entities.tail:_*)
      Update[E](insertSql)(entityComposite).updateMany(nel)
    }
  }

  def insertReturningKey[EList<:HList,REMList<:HList,REM](entity: E)
  (implicit eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    remComposite: Composite[REMList],
    remTupler: hlist.Tupler.Aux[REMList,REM]
  ): ConnectionIO[K] = {
    val frag = insertFragment(entity)
    if (!autogen) frag.run.map(_ => primaryKey(entity))
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
      doobieQuery(k,keyComposite).query[E](entityComposite).unique
    dialect match {
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
    def f(e: E) = updater.update(this,e)
    val first = f(entities.head)
    if (entities.tail.size == 0) first
    else entities.tail.foldLeft(first)( (io,e) =>
        io.flatMap(i => f(e).map(_+i))
    )
  }

  private[scarm] lazy val updateSql: String = {
    val updatedCols = nonKeyFieldNames.map(f => s"${f}=?").mkString(",")
    s"UPDATE ${name} SET ${updatedCols} WHERE ${whereClauseNoAlias}"
  }

}


object Table {

  private def pkeyColumns[K,E](kmap: FieldMap[K], fmap: FieldMap[E]) =
    kmap.names.map(name => fmap.firstFieldName +
      //if the name is empty, the key is atomic.  Otherwise it's a case class.
      (if (name == "") "" else "_" + name)
    )

  def apply[K,E](name: String)
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E]
    ): Table[K,E] = {
    Table[K,E](name, fmap, pkeyColumns(kmap,fmap), false, dialect, kcomp,
      ecomp, primaryKey)
  }

  def apply[K,E](name: String, kNames: Seq[String])
    (implicit dialect: SqlDialect,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E]
    ): Table[K,E] =
    Table[K,E](name, fmap, kNames, false, dialect, kcomp,
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

  private def typeName(dialect: SqlDialect, table: Table[_,_],
    item: FieldMap.Item, overrides: Map[Type,String]) =  {
    val nullable = if (item.optional) "" else "not null"
    val auto =
      if (!table.autogen || item.name != table.autogenField) ""
      else autogenModifier(dialect, table.name, item.name)
    overrides.getOrElse(item.tpe, sqlTypeMap(dialect).get(item.tpe.typeSymbol)) match {
      case None => throw new RuntimeException(s"Could not find sql type for type ${item.tpe.companion}")
      case Some(typeString) => s"${typeString} ${nullable} ${auto}"
    }
  }

  def createSql[K,E](
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
    (implicit dialect: SqlDialect,
      kmap: FieldMap[K],
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E]
    ): Table[K,E] = Table[K,E](name).copy(autogen=true)

  def apply[K,E](name: String, kNames: Seq[String])
    (implicit dialect: SqlDialect,
      fmap: FieldMap[E],
      kcomp: Composite[K],
      ecomp: Composite[E],
      primaryKey: PrimaryKey[K,E],
    ): Table[K,E] = Table[K,E](name,kNames).copy(autogen=true)
}


case class TableScan[K,E](table: Table[K,E])
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
  val definition: String,
  override val keyNames: Seq[String],
  val fieldNames: Seq[String]
) extends Queryable[K,Set,E,E] {

  override private[scarm] def selectList(ct: Int): String = 
    fieldNames.map(f => s"${tname(ct)}.${f}").mkString(",")

  override private[scarm] def tablect: Int = 1

  override private[scarm] def tableList(ct: Int = 0): Seq[String] =
    Seq(alias(s"(${definition})", ct))

  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows

  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] =
    reduced.toSet
}

object View {
  def apply[K,E](definition: String)
    (implicit fmap: FieldMap[E], kmap: FieldMap[K]): View[K,E] = 
    View(definition, kmap.names, fmap.names)
}


trait IndexBase[K,PK,E] {
  def name: String
  def table: Table[PK,E]
  def keyNames: Seq[String]
  def unique: Boolean

  def create: ConnectionIO[Int] = {
    val keys = keyNames.mkString(",")
    val uniqueness = if (unique) "UNIQUE" else ""
    val sql = s"CREATE ${uniqueness} INDEX ${name} on ${table.name} (${keys})"
    Fragment(sql, ()).update.run
  }
}

case class Index[K,PK,E](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Set,E,E] with IndexBase[K,PK,E]{
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String ]= Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Set[T] = reduced.toSet
  override def unique: Boolean = false
}


object Index {

  private[scarm] def indexName(table: Table[_,_], ttag: TypeTag[_]) =
    table.name + "_" + ttag.tpe.typeSymbol.name + "_idx"

  def apply[K,PK,E](table: Table[PK,E])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K], ktag: TypeTag[K])
      : Index[K,PK,E] = Index(indexName(table,ktag), table, kmap.names)

  def apply[K,PK,E](table: Table[PK,E], keyClass: Class[K])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K], ktag: TypeTag[K])
      : Index[K,PK,E] = Index(indexName(table,ktag), table, kmap.names)

  def apply[K,PK,E](name: String, table: Table[PK,E])
    (implicit  isProjection: Subset[K,E], kmap: FieldMap[K])
      : Index[K,PK,E] = Index(name, table, kmap.names)

  def tupled[K,PK,E,KList<:HList, T<:Product](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K],
    kgen: Generic.Aux[K,KList],
    tupled: hlist.Tupler.Aux[KList,T]
  ): Index[T,PK,E] = Index(indexName(table,ktag), table, kmap.names)
}


case class UniqueIndex[K,PK,E](
  override val name: String,
  table: Table[PK,E],
  override val keyNames: Seq[String]
) extends DatabaseObject with Queryable[K,Option,E,E] with IndexBase[K,PK,E] {
  override private[scarm] def selectList(ct: Int): String = table.selectList(ct)
  override private[scarm] def tableList(ct: Int=0): Seq[String] = Seq(alias(table.name, ct))
  override private[scarm] def reduceResults(rows: Traversable[E]): Traversable[E] = rows
  override private[scarm] def collectResults[T](reduced: Traversable[T]): Option[T] =
    if (reduced.isEmpty) None else Some(reduced.head)

  override def unique: Boolean = true
}


object UniqueIndex {

  def apply[K,PK,E,KList<:HList,EList<:HList](table: Table[PK,E])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K], ktag: TypeTag[K])
      : UniqueIndex[K,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, kmap.names)

  def apply[K,PK,E](table: Table[PK,E], keyClass: Class[K])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K], ktag: TypeTag[K])
      : UniqueIndex[K,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, kmap.names)

  def apply[K,PK,E,KList<:HList,EList<:HList](name: String,  table: Table[PK,E])
    (implicit isProjection: Subset[K,E], kmap: FieldMap[K])
      : UniqueIndex[K,PK,E] = UniqueIndex(name, table, kmap.names)

  def tupled[K,PK,E,KList<:HList, T<:Product](table: Table[PK,E], keyClass: Class[K])(implicit
    isProjection: Subset[K,E],
    kmap: FieldMap[K],
    ktag: TypeTag[K],
    kgen: Generic.Aux[K,KList],
    tupled: hlist.Tupler.Aux[KList,T]
  ): UniqueIndex[T,PK,E] = UniqueIndex(Index.indexName(table,ktag), table, kmap.names)
}


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
    val fknames = fkmap.names
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
    val fknames = fkmap.names
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
