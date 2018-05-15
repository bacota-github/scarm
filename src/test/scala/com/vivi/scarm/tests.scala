package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.sql.{ SQLException }
import java.time._

import org.scalatest._

import com.vivi.scarm._
import com.vivi.scarm.FieldMap._

import TestObjects._
import com.vivi.scarm.JavaTimeLocalDateMeta

object DSLSuite {
  val hsqldbCleanup = (xa:Transactor[IO]) => {
    val op = for {
      _ <- sql"DROP SCHEMA PUBLIC CASCADE".update.run
      s <- sql"SHUTDOWN IMMEDIATELY".update.run
    } yield false
    op.transact(xa).unsafeRunSync()
  }
}

case class StringId(id: String) extends AnyVal

case class StringKeyEntity(id: StringId, name: String) extends Entity[StringId]

case class Id(id: Int) extends AnyVal
case class PrimitiveEntity(id: Id, name: String) extends Entity[Id]

case class InnerKey(x: Short, y: Short)
case class CompositeKey(first: Long, inner: InnerKey, last: String)
case class CompositeKeyEntity(id: CompositeKey, name: String)
    extends Entity[CompositeKey]

case class EntityWithAllPrimitiveTypes(id: Id, char: Char, string: String,
  boolean:Boolean,short: Short, int: Int, long: Long, float: Float,
  double: Double)  extends Entity[Id]

case class DateEntity(id: Id,
  instant: Instant = Instant.now,
  localDate: LocalDate = LocalDate.now,
  localDateTime: LocalDateTime = LocalDateTime.now,
  localTimex: LocalTime = LocalTime.now
) extends Entity[Id]

case class Level2(x: Id, y: String)
case class Level1(x: Int, y: Int, level2: Level2)
case class DeeplyNestedEntity(id: Id, x: Int, nested: Level1)
    extends Entity[Id]

case class NullableEntity(id: Id, name: Option[String])
    extends Entity[Id]

case class NullableNestedEntity(id: Id, nested: Option[Level1])
    extends Entity[Id]

class DSLSuite extends Suites(
  new DSLTest("org.hsqldb.jdbc.JDBCDriver",
    "jdbc:hsqldb:file:testdb",
    "SA", "", Hsqldb,
    DSLSuite.hsqldbCleanup),

  new DSLTest("org.postgresql.Driver",
    "jdbc:postgresql:scarm", "scarm", "scarm", Postgresql,
  ),

  new DSLTest("com.mysql.cj.jdbc.Driver",
    "jdbc:mysql://localhost:3306/scarm?serverTimezone=UTC&useSSL=false&sql_mode=",
    "scarm", "scarm", Mysql
  )
)


case class DSLTest(driver: String,
  url: String,
  username: String,
  pass: String,
  dialect: SqlDialect,
  cleanup: (Transactor[IO] => Boolean) = (_ => true ) 
) extends FunSuite with BeforeAndAfterAll {

  implicit val theDialect: SqlDialect = dialect

  info(s"Testing with url ${url}")

  implicit val xa = Transactor.fromDriverManager[IO](driver, url, username, pass)

  private def run[T](op: ConnectionIO[T]): T = op.transact(xa).unsafeRunSync()

  private def runQuietly(op: ConnectionIO[_]) = try {
    run(op)
  } catch { case _:Exception => }


  val stringTable = Table[StringId,StringKeyEntity]("string")
  val primitiveTable = Table[Id,PrimitiveEntity]("primitive")
  val compositeTable = Table[CompositeKey,CompositeKeyEntity]("composite")
  val autogenTable = Autogen[Id,PrimitiveEntity]("autogen")
  val nestedTable = Table[Id,DeeplyNestedEntity]("nested")
  val nullableTable = Table[Id,NullableEntity]("nullable")
  val nullableNestedTable = Table[Id,NullableNestedEntity]("nullable_nested")
  val dateTable = Table[Id,DateEntity]("date")
  //  val primitivesTable = Table[Id,EntityWithAllPrimitiveTypes]("primtive")

  val allTables = Seq(stringTable,primitiveTable,compositeTable, autogenTable,
    nestedTable, nullableTable,nullableNestedTable, dateTable)

  private def createAll() = 
    for (t <- allTables) {
      t.create().transact(xa).unsafeRunSync()
    }

  private def dropAll(xa: Transactor[IO]): Unit = 
    for (t <-allTables) {
      try {
        t.dropCascade.transact(xa).unsafeRunSync()
      } catch { case e: Exception =>
          println(s"failed dropping ${t.name} ${e.getMessage()}")
      }
    }

  override def beforeAll() {
    createAll()
  }

  override def afterAll() {
    cleanup(xa)
    if (cleanup(xa))  dropAll(xa) else ()
  }

  private val rand = new java.util.Random()
  private def randomString: String = java.util.UUID.randomUUID().toString
  private def randomCompositeKey = {
    val first = rand.nextLong()
    val last = randomString
    val innerKey = InnerKey(rand.nextInt().toShort, rand.nextInt().toShort)
    CompositeKey(first, innerKey, last)
  }

  test("After inserting an entity into a table with String primary key, the entity can be selected")  {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    run(for {
      i1 <- stringTable.insert(e1)
      i2 <- stringTable.insert(e2)
      i3 <- stringTable.insert(e3)
      e2New <- stringTable(e2.id)
      e1New <- stringTable(e1.id)
      e3New <- stringTable(e3.id)
    } yield {
      assert (i1 == 1)
      assert (i2 == 1)
      assert (i3 == 1)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("After inserting a batch of entities into a table with String primary key, every entity can be selected") {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    run(for {
      i <- stringTable.insertBatch(e1,e2,e3)
      e2New <- stringTable(e2.id)
      e1New <- stringTable(e1.id)
      e3New <- stringTable(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with String primary key returns the correct Key and the entity can be selected") {
    val e = StringKeyEntity(StringId(randomString), randomString)
    run(for {
      k <- stringTable.insertReturningKey(e)
      eNew <- stringTable(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertBatchReturningKey on entities with String primary key returns the correct Keys and the entities can be selected") {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(stringTable.insertBatchReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(stringTable(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with String primary key returns the entity and the entity can be selected") {
    val e = StringKeyEntity(StringId(randomString), randomString)
    run(for {
      returned <- stringTable.insertReturning(e)
      selected <- stringTable(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertBatchReturning entities with String primary key returns the entities and the entities can be selected") {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(stringTable.insertBatchReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(stringTable(e.id)) == Some(e))
    }
  }

  test("after deleting by String primary key, selecting on those keys returns None") {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    assert(run(stringTable.insertBatch(e1,e2,e3)) == 3)
    assert(run(stringTable.delete(e1.id,e2.id)) == 2)
    assert(run(stringTable(e1.id)) == None)
    assert(run(stringTable(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(stringTable(e3.id)) == Some(e3))
  }


  test("updates of entities with String primary key are reflected in future selects") {
    val e1 = StringKeyEntity(StringId(randomString), randomString)
    val e2 = StringKeyEntity(StringId(randomString), randomString)
    val e3 = StringKeyEntity(StringId(randomString), randomString)
    assert(run(stringTable.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(stringTable.update(update1, update2)) == 2)
    assert(run(stringTable(e1.id)) == Some(update1))
    assert(run(stringTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(stringTable(e3.id)) == Some(e3))
  }


  var nextIdVal = 0
  def nextId = {
    nextIdVal += 1
    Id(nextIdVal)
  }
  
  test("After inserting an entity into a table with primitive primary key, the entity can be selected")  {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    run(for {
      i1 <- primitiveTable.insert(e1)
      i2 <- primitiveTable.insert(e2)
      i3 <- primitiveTable.insert(e3)
      e2New <- primitiveTable(e2.id)
      e1New <- primitiveTable(e1.id)
      e3New <- primitiveTable(e3.id)
    } yield {
      assert (i1 == 1)
      assert (i2 == 1)
      assert (i3 == 1)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("After inserting a batch of entities into a table with primitive primary key, every entity can be selected") {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    run(for {
      i <- primitiveTable.insertBatch(e1,e2,e3)
      e2New <- primitiveTable(e2.id)
      e1New <- primitiveTable(e1.id)
      e3New <- primitiveTable(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with primitive primary key returns the correct Key and the entity can be selected") {
    val e = PrimitiveEntity(nextId, randomString)
    run(for {
      k <- primitiveTable.insertReturningKey(e)
      eNew <- primitiveTable(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertBatchReturningKey on entities with primitive primary key returns the correct Keys and the entities can be selected") {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(primitiveTable.insertBatchReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(primitiveTable(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with primitive primary key returns the entity and the entity can be selected") {
    val e = PrimitiveEntity(nextId, randomString)
    run(for {
      returned <- primitiveTable.insertReturning(e)
      selected <- primitiveTable(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertBatchReturning entities with primitive primary key returns the entities and the entities can be selected") {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(primitiveTable.insertBatchReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(primitiveTable(e.id)) == Some(e))
    }
  }

  test("after deleting by primitive primary key, selecting on those keys returns None") {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    assert(run(primitiveTable.insertBatch(e1,e2,e3)) == 3)
    assert(run(primitiveTable.delete(e1.id,e2.id)) == 2)
    assert(run(primitiveTable(e1.id)) == None)
    assert(run(primitiveTable(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(primitiveTable(e3.id)) == Some(e3))
  }

  test("updates of entities with primitive primary key are reflected in future selects") {
    val e1 = PrimitiveEntity(nextId, randomString)
    val e2 = PrimitiveEntity(nextId, randomString)
    val e3 = PrimitiveEntity(nextId, randomString)
    assert(run(primitiveTable.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(primitiveTable.update(update1, update2)) == 2)
    assert(run(primitiveTable(e1.id)) == Some(update1))
    assert(run(primitiveTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(primitiveTable(e3.id)) == Some(e3))
  }


  test("After inserting an entity into a table with composite key, the entity can be selected")  {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    run(for {
      i1 <- compositeTable.insert(e1)
      i2 <- compositeTable.insert(e2)
      i3 <- compositeTable.insert(e3)
      e2New <- compositeTable(e2.id)
      e1New <- compositeTable(e1.id)
      e3New <- compositeTable(e3.id)
    } yield {
      assert (i1 == 1)
      assert (i2 == 1)
      assert (i3 == 1)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("After inserting a batch of entities into a table with composite key, every entity can be selected") {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    run(for {
      i <- compositeTable.insertBatch(e1,e2,e3)
      e2New <- compositeTable(e2.id)
      e1New <- compositeTable(e1.id)
      e3New <- compositeTable(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with composite key returns the correct Key and the entity can be selected") {
    val e = CompositeKeyEntity(randomCompositeKey, randomString)
    run(for {
      k <- compositeTable.insertReturningKey(e)
      eNew <- compositeTable(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertBatchReturningKey on entities with composite key returns the correct Keys and the entities can be selected") {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(compositeTable.insertBatchReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(compositeTable(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with composite key returns the entity and the entity can be selected") {
    val e = CompositeKeyEntity(randomCompositeKey, randomString)
    run(for {
      returned <- compositeTable.insertReturning(e)
      selected <- compositeTable(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertBatchReturning entities with composite key returns the entities and the entities can be selected") {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(compositeTable.insertBatchReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(compositeTable(e.id)) == Some(e))
    }
  }

  test("after deleting by composite key, selecting on those keys returns None") {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    assert(run(compositeTable.insertBatch(e1,e2,e3)) == 3)
    assert(run(compositeTable.delete(e1.id,e2.id)) == 2)
    assert(run(compositeTable(e1.id)) == None)
    assert(run(compositeTable(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(compositeTable(e3.id)) == Some(e3))
  }


  test("updates of entities with composite key are reflected in future selects") {
    val e1 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e2 = CompositeKeyEntity(randomCompositeKey, randomString)
    val e3 = CompositeKeyEntity(randomCompositeKey, randomString)
    assert(run(compositeTable.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(compositeTable.update(update1, update2)) == 2)
    assert(run(compositeTable(e1.id)) == Some(update1))
    assert(run(compositeTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(compositeTable(e3.id)) == Some(e3))
  }


  test("insertReturningKey of an entity with autogen primary key returns the correct Key and the entity can be selected") {
    val e = PrimitiveEntity(Id(0), randomString)
    run(for {
      k <- autogenTable.insertReturningKey(e)
      eNew <- autogenTable(k)
    } yield {
      assert(eNew == Some(e.copy(id=k)))
    })
  }


  test("insertBatchReturningKey on entities with autogen primary key returns the correct Keys and the entities can be selected") {
    val e1 = PrimitiveEntity(Id(0), randomString)
    val e2 = PrimitiveEntity(Id(0), randomString)
    val e3 = PrimitiveEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(autogenTable.insertBatchReturningKeys(e1,e2,e3))
    val zipped = keys zip entities
    for ((k,e) <- zipped) {
      val readName = run(autogenTable(k)).get.name
      assert(e.name == readName)
    }
  }

  test("insertReturning an entity with autogen primary key returns the entity and the entity can be selected") {
    val e = PrimitiveEntity(Id(0), randomString)
    run(for {
      returned <- autogenTable.insertReturning(e)
      selected <- autogenTable(returned.id)
    } yield {
      assert(returned.name == e.name)
      assert(selected.get.name == e.name)
    })
  }

  test("insertBatchReturning entities with autogen primary key returns the entities and the entities can be selected") {
    val e1 = PrimitiveEntity(Id(0), randomString)
    val e2 = PrimitiveEntity(Id(0), randomString)
    val e3 = PrimitiveEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(autogenTable.insertBatchReturning(e1,e2,e3))
    val zipped = returned zip entities
    for ((ret, e) <- zipped) {
      assert(ret.name == e.name)
      assert(run(autogenTable(ret.id)).get.name == e.name)
    }
  }

  test("after deleting by autogen primary key, selecting on those keys returns None") {
    val e1 = PrimitiveEntity(Id(0), randomString)
    val e2 = PrimitiveEntity(Id(0), randomString)
    val e3 = PrimitiveEntity(Id(0), randomString)
    val keys = run(autogenTable.insertBatchReturningKeys(e1,e2,e3))
    assert(run(autogenTable.delete(keys(0), keys(1))) == 2)
    assert(run(autogenTable(keys(0))) == None)
    assert(run(autogenTable(keys(1))) == None)
    //sneak in a test for accidental deletion
    assert(run(autogenTable(keys(2))).get.name == e3.name)
  }

  test("updates of entities with autogen primary key are reflected in future selects") {
    val entities = Seq(
      PrimitiveEntity(Id(0), randomString),
      PrimitiveEntity(Id(0), randomString),
      PrimitiveEntity(Id(0), randomString)
    )
    val Seq(e1,e2,e3) = run(autogenTable.insertBatchReturning(entities:_*))
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(autogenTable.update(update1, update2)) == 2)
    assert(run(autogenTable(e1.id)) == Some(update1))
    assert(run(autogenTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(autogenTable(e3.id)) == Some(e3))
  }


  test("A table scan returns all the entities in the table") {
    val table = Table[Id,PrimitiveEntity]("scan_test")
    try {
      val entities = Set(PrimitiveEntity(nextId,randomString),
        PrimitiveEntity(nextId, randomString),
        PrimitiveEntity(nextId, randomString)
      )
      val op = for {
        _ <- table.create()
        _ <- table.insertBatch(entities.toSeq: _*)
        results <- table.scan(Unit)
      } yield {
        assert(results == entities)
      }
      run(op)
    } finally {
      runQuietly(table.drop)
    }
  }


  test("deleting a nonexistent entity affects nothing") {
    val n = primitiveTable.delete(Id(-1))
    assert (run(n) == 0)
  }

  test("updating a nonexistent entity affects nothing") {
    val t = PrimitiveEntity(Id(-1), randomString)
    assert(0 == run(primitiveTable.update(t)))
  }


  test("a dropped table cannot be used") { 
    val table = Table[Id,PrimitiveEntity]("drop_test")
    run(table.create())
    runQuietly(table.drop)
    assertThrows[SQLException] {
      run(table.insert(PrimitiveEntity(Id(1),"foo")))
    }
  }


  test("java.time fields are supported") {
    val newDate = DateEntity(nextId)
    val table = dateTable
    run(table.insert(newDate))
    val readDate = run(table(newDate.id))
    assert(!readDate.isEmpty)
    val read = readDate.get

    val originalEpoch = newDate.instant.getEpochSecond()
    val readEpoch = read.instant.getEpochSecond()
    assert(Math.abs(readEpoch - originalEpoch) <= 1)
    assert(read.localDate == newDate.localDate)

    val rdate = read.localDateTime
    val ndate = newDate.localDateTime
    assert(rdate.getYear() == ndate.getYear())
    assert(rdate.getMonth() == ndate.getMonth())
    assert(rdate.getDayOfMonth() == ndate.getDayOfMonth())
    assert(rdate.getDayOfWeek() == ndate.getDayOfWeek())
    assert(rdate.getDayOfYear() == ndate.getDayOfYear())
    assert(rdate.getHour() == ndate.getHour())
    assert(rdate.getMinute() == ndate.getMinute())
    assert(Math.abs(rdate.getSecond() - ndate.getSecond()) <= 1)

    val rtime = read.localTimex
    val ntime = newDate.localTimex
    assert(rtime.getHour() == ntime.getHour())
    assert(rtime.getMinute() == ntime.getMinute())
    assert(rtime.getSecond() == ntime.getSecond())
  }


  test("entities with nested objects supported")  (pending)

  test("various primitive fields supported")   (pending)

  test("entities with nullable (Option) fields can be inserted, selected, and updated")   (pending)

  test("entities with nullable (Option) nested fields can be inserted, selected, and updated")   (pending)

  test("SQL on a table with explicit key names") (pending)

  test("Query by Index") (pending)

  test("Query by Index with no results") (pending)

 test("Query by index returns only entities with correct key") (pending)

 test("Query by Unique Index") (pending)

 test("Query by Unique index returns only entities with correct key") (pending)

  test("Query by Unique Index with no results") (pending)

  test("Query by Foreign Key") (pending)

 test("Query by Foreign Key returns only entities with correct key") (pending)

  test("Query a View") (pending)

  test("A mandatory foreign key is a constraint")(pending)

  test("An optional foreign key is a constraint")(pending)

  test("An optional foreign key is optional")(pending)

  test("Query a Many to One Join on Mandatory Foreign Key") (pending)

  test("Many to One Join on Mandatory Foreign Key is Inner") (pending)

  test("Query a Many to One Join on Optional Foreign Key") (pending)

  test("Many to One Join on Optional Foreign Key is Outer") (pending)

  test("Query a One to Many Join") (pending)

  test("One to Many Join is Outer") (pending)

  test("Query three queries joined by many to one") (pending)

  test("Query three queries joined by one to many") (pending)

  test("Query three queries joined by one to many and many to one") (pending)

  test("Query three queries joined by many to one and one to many") (pending)

  test("Query with a Nested Join") (pending)

  test("Query with Join with compound primary key") (pending)

  test("Query with Join with primary key containing date") (pending)

  test("Query with Join with primitive primary key") (pending)

  test("Query with Join with compound primary key containing nested object") (pending)

  test("field name overrides work") (pending)

  test("sql type overrides work") (pending)
}
