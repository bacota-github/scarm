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

import shapeless._

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



class DSLSuite extends Suites(
  DSLTest("org.hsqldb.jdbc.JDBCDriver",
    "jdbc:hsqldb:file:testdb",
    "SA", "", Hsqldb,
    DSLSuite.hsqldbCleanup),

  DSLTest("org.postgresql.Driver",
    "jdbc:postgresql:scarm", "scarm", "scarm", Postgresql,
  ),

  DSLTest("com.mysql.cj.jdbc.Driver",
    "jdbc:mysql://localhost:3306/scarm?serverTimezone=UTC&useSSL=false&sql_mode=",
    "scarm", "scarm", Mysql
  )
)

object DSLTest {
  def xa(driver: String, url: String, username: String, pass: String)
  = Transactor.fromDriverManager[IO](driver, url, username, pass)
}

case class DSLTest(driver: String,
  url: String,
  username: String,
  pass: String,
  dialect: SqlDialect,
  cleanup: (Transactor[IO] => Boolean) = (_ => true ) 
) extends Suites(
  TestWithStringPrimaryKey(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestWithPrimitivePrimaryKey(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestWithCompositePrimaryKey(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestAutogen(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestMiscellaneous(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestJavaTime(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestNestedObjectTable(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestAllPrimitivesTable(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestNullableFields(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestIndex(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestUniqueIndex(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  ForeignKeyTests(DSLTest.xa(driver,url,username,pass),dialect,cleanup)
)


trait DSLTestBase extends Suite with BeforeAndAfterAll {
  def dialect: SqlDialect
  implicit def implicitDialect = dialect

  def xa: Transactor[IO]
  implicit def implicitXA = xa

  def allTables: Seq[Table[_,_]]
  def cleanup: (Transactor[IO] => Boolean) = (_ => true ) 

  def run[T](op: ConnectionIO[T]): T = op.transact(xa).unsafeRunSync()

  def runQuietly(op: ConnectionIO[_]) = try {
    run(op)
  } catch { case _:Exception => }

  def createAll: Unit =  
    for (t <- allTables) {
      t.create().transact(xa).unsafeRunSync()
    }

  def dropAll: Unit = 
    for (t <-allTables) {
      try {
        t.dropCascade.transact(xa).unsafeRunSync()
      } catch { case e: Exception =>
          println(s"failed dropping ${t.name} ${e.getMessage()}")
      }
    }

   override def beforeAll() {
    createAll
  }

   override def afterAll() {
    if (cleanup(xa)) dropAll else ()
   }

  val rand = new java.util.Random()
  def randomString: String = java.util.UUID.randomUUID().toString

  var nextIdVal = 0
  def nextId = {
    nextIdVal += 1
    Id(nextIdVal)
  }
}


case class StringId(id: String) extends AnyVal
case class StringKeyEntity(id: StringId, name: String)

case class TestWithStringPrimaryKey(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true ) 
) extends FunSuite with DSLTestBase  {
  val stringTable = Table[StringId,StringKeyEntity]("string")
  override val  allTables = Seq(stringTable)

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
}

case class Id(id: Int) extends AnyVal
case class IntEntity(id: Id, name: String)

case class TestWithPrimitivePrimaryKey(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val intTable = Table[Id,IntEntity]("int_table")
  override val  allTables = Seq(intTable)

  test("After inserting an entity into a table with primitive primary key, the entity can be selected")  {
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    run(for {
      i1 <- intTable.insert(e1)
      i2 <- intTable.insert(e2)
      i3 <- intTable.insert(e3)
      e2New <- intTable(e2.id)
      e1New <- intTable(e1.id)
      e3New <- intTable(e3.id)
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
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    run(for {
      i <- intTable.insertBatch(e1,e2,e3)
      e2New <- intTable(e2.id)
      e1New <- intTable(e1.id)
      e3New <- intTable(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with primitive primary key returns the correct Key and the entity can be selected") {
    val e = IntEntity(nextId, randomString)
    run(for {
      k <- intTable.insertReturningKey(e)
      eNew <- intTable(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertBatchReturningKey on entities with primitive primary key returns the correct Keys and the entities can be selected") {
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(intTable.insertBatchReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(intTable(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with primitive primary key returns the entity and the entity can be selected") {
    val e = IntEntity(nextId, randomString)
    run(for {
      returned <- intTable.insertReturning(e)
      selected <- intTable(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertBatchReturning entities with primitive primary key returns the entities and the entities can be selected") {
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(intTable.insertBatchReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(intTable(e.id)) == Some(e))
    }
  }

  test("after deleting by primitive primary key, selecting on those keys returns None") {
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    assert(run(intTable.insertBatch(e1,e2,e3)) == 3)
    assert(run(intTable.delete(e1.id,e2.id)) == 2)
    assert(run(intTable(e1.id)) == None)
    assert(run(intTable(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(intTable(e3.id)) == Some(e3))
  }

  test("updates of entities with primitive primary key are reflected in future selects") {
    val e1 = IntEntity(nextId, randomString)
    val e2 = IntEntity(nextId, randomString)
    val e3 = IntEntity(nextId, randomString)
    assert(run(intTable.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(intTable.update(update1, update2)) == 2)
    assert(run(intTable(e1.id)) == Some(update1))
    assert(run(intTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(intTable(e3.id)) == Some(e3))
  }
}


case class InnerKey(x: Short, y: Short)
case class CompositeKey(first: Long, inner: InnerKey, last: String)
case class CompositeKeyEntity(id: CompositeKey, name: String)

case class TestWithCompositePrimaryKey(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val compositeTable = Table[CompositeKey,CompositeKeyEntity]("composite")
  override val  allTables = Seq(compositeTable)

  private def randomCompositeKey = {
    val first = rand.nextLong()
    val last = randomString
    val innerKey = InnerKey(rand.nextInt().toShort, rand.nextInt().toShort)
    CompositeKey(first, innerKey, last)
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
}


case class TestAutogen(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val autogenTable = Autogen[Id,IntEntity]("autogen")
  override val  allTables = Seq(autogenTable)

  test("insertReturningKey of an entity with autogen primary key returns the correct Key and the entity can be selected") {
    val e = IntEntity(Id(0), randomString)
    run(for {
      k <- autogenTable.insertReturningKey(e)
      eNew <- autogenTable(k)
    } yield {
      assert(eNew == Some(e.copy(id=k)))
    })
  }


  test("insertBatchReturningKey on entities with autogen primary key returns the correct Keys and the entities can be selected") {
    val e1 = IntEntity(Id(0), randomString)
    val e2 = IntEntity(Id(0), randomString)
    val e3 = IntEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(autogenTable.insertBatchReturningKeys(e1,e2,e3))
    val zipped = keys zip entities
    for ((k,e) <- zipped) {
      val readName = run(autogenTable(k)).get.name
      assert(e.name == readName)
    }
  }

  test("insertReturning an entity with autogen primary key returns the entity and the entity can be selected") {
    val e = IntEntity(Id(0), randomString)
    run(for {
      returned <- autogenTable.insertReturning(e)
      selected <- autogenTable(returned.id)
    } yield {
      assert(returned.name == e.name)
      assert(selected.get.name == e.name)
    })
  }

  test("insertBatchReturning entities with autogen primary key returns the entities and the entities can be selected") {
    val e1 = IntEntity(Id(0), randomString)
    val e2 = IntEntity(Id(0), randomString)
    val e3 = IntEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(autogenTable.insertBatchReturning(e1,e2,e3))
    val zipped = returned zip entities
    for ((ret, e) <- zipped) {
      assert(ret.name == e.name)
      assert(run(autogenTable(ret.id)).get.name == e.name)
    }
  }

  test("after deleting by autogen primary key, selecting on those keys returns None") {
    val e1 = IntEntity(Id(0), randomString)
    val e2 = IntEntity(Id(0), randomString)
    val e3 = IntEntity(Id(0), randomString)
    val keys = run(autogenTable.insertBatchReturningKeys(e1,e2,e3))
    assert(run(autogenTable.delete(keys(0), keys(1))) == 2)
    assert(run(autogenTable(keys(0))) == None)
    assert(run(autogenTable(keys(1))) == None)
    //sneak in a test for accidental deletion
    assert(run(autogenTable(keys(2))).get.name == e3.name)
  }

  test("updates of entities with autogen primary key are reflected in future selects") {
    val entities = Seq(
      IntEntity(Id(0), randomString),
      IntEntity(Id(0), randomString),
      IntEntity(Id(0), randomString)
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
}

case class TestMiscellaneous(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val intTable = Table[Id,IntEntity]("misc_test")
  override val  allTables = Seq(intTable)

  test("A table scan returns all the entities in the table") {
    val table = Table[Id,IntEntity]("scan_test")
    try {
      val entities = Set(IntEntity(nextId,randomString),
        IntEntity(nextId, randomString),
        IntEntity(nextId, randomString)
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
    val n = intTable.delete(Id(-1))
    assert (run(n) == 0)
  }

  test("updating a nonexistent entity affects nothing") {
    val t = IntEntity(Id(-1), randomString)
    assert(0 == run(intTable.update(t)))
  }

  test("a dropped table cannot be used") { 
    val table = Table[Id,IntEntity]("drop_test")
    run(table.create())
    runQuietly(table.drop)
    assertThrows[SQLException] {
      run(table.insert(IntEntity(Id(1),"foo")))
    }
  }
}

case class DateEntity(id: Id,
  instant: Instant = Instant.now,
  localDate: LocalDate = LocalDate.now,
  localDateTime: LocalDateTime = LocalDateTime.now,
  localTimex: LocalTime = LocalTime.now
)


case class TestJavaTime(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val dateTable = Table[Id,DateEntity]("date")
  override val  allTables = Seq(dateTable)

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
}

case class Level2(x: Int, y: String)
case class Level1(x: Int, y: Int, level2: Level2)
case class DeeplyNestedEntity(id: Id, x: Int, nested: Level1)

case class TestNestedObjectTable(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val nestedTable = Table[Id,DeeplyNestedEntity]("nested")
  override val  allTables = Seq(nestedTable)

  test("entities with nested objects supported") {
    val entity = DeeplyNestedEntity(nextId, 1, Level1(2,3, Level2(4, "hi")))
    assert(run(nestedTable.insert(entity)) == 1)
    assert(run(nestedTable(entity.id)) == Some(entity))
  }
}


case class EntityWithAllPrimitiveTypes(id: Id,
  stringCol: String,
  booleanCol: Boolean,
  shortCol: Short,
  intCol: Int,
  longCol: Long,
  floatCol: Float,
  doubleCol: Double)

case class TestAllPrimitivesTable(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val primitivesTable = Table[Id,EntityWithAllPrimitiveTypes]("allPrimitives")
  override val allTables = Seq(primitivesTable)

  test("various primitive fields supported") {
    val entity = EntityWithAllPrimitiveTypes(nextId,randomString,
      true, 1, 2, 3, 1.0f, 2.0)
    assert(run(primitivesTable.insert(entity)) == 1)
    assert(run(primitivesTable(entity.id)) == Some(entity))
  }
}


case class NullableEntity(id: Id, name: Option[String])

case class NullableNestedEntity(id: Id, nested: Option[Level1])

case class TestNullableFields(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val nullableTable = Table[Id,NullableEntity]("nullable")
  val nullableNestedTable = Table[Id,NullableNestedEntity]("nullable_nested")
  override val allTables = Seq(nullableTable, nullableNestedTable)


  test("entities with nullable (Option) fields can be inserted, selected, and updated")  {
    val e1 = NullableEntity(nextId, Some(randomString))
    val e2 = NullableEntity(nextId, None)
    assert(run(nullableTable.insertBatch(e1,e2)) == 2)
    assert(run(nullableTable(e1.id)) == Some(e1))
    assert(run(nullableTable(e2.id)) == Some(e2))
    val updated1 = NullableEntity(e1.id, None)
    val updated2 = NullableEntity(e2.id, Some(randomString))
    assert(run(nullableTable.update(updated1, updated2)) == 2)
    assert(run(nullableTable(e1.id)) == Some(updated1))
    assert(run(nullableTable(e2.id)) == Some(updated2))
  }

  test("entities with nullable (Option) nested fields can be inserted, selected, and updated") {
    val e1 = NullableNestedEntity(nextId, Some(Level1(1,2,Level2(3,"4"))))
    val e2 = NullableNestedEntity(nextId, None)
    assert(run(nullableNestedTable.insertBatch(e1,e2)) == 2)
    assert(run(nullableNestedTable(e1.id)) == Some(e1))
    assert(run(nullableNestedTable(e2.id)) == Some(e2))
    val updated1 = NullableNestedEntity(e1.id, None)
    val updated2 = NullableNestedEntity(e2.id, Some(Level1(2,3,Level2(4,"5"))))
    assert(run(nullableNestedTable.update(updated1, updated2)) == 2)
    assert(run(nullableNestedTable(e1.id)) == Some(updated1))
    assert(run(nullableNestedTable(e2.id)) == Some(updated2))
  }
}

case class MultiEntity(id: Id, name: String, x: Option[Int])
case class MultiIndexKey(x: Option[Int], name: String) 
case class MultiIndexKeyNotQuiteRight(x: Int, nme: String)

case class NestedField(x: Int, y: String)
case class NestedEntity(id: Id, nested: NestedField)

case class NestedKey(x: Int)
case class NestedIndexKey(nested: NestedKey)
case class NotQuiteRightNestedIndexKey(nestd: NestedKey)


case class TestIndex(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val multiTable = Table[Id,MultiEntity]("multi")
  val nestedTable = Table[Id,NestedEntity]("nestedTable")
  override val allTables = Seq(multiTable,nestedTable)

  test("Query by multi-column Index") {
    val index: Index[MultiIndexKey,Id,MultiEntity] = Index(multiTable)
    val name = randomString
    val e1 = MultiEntity(nextId, name, Some(1))
    val e2 = MultiEntity(nextId, randomString, Some(2))
    val e3 = MultiEntity(nextId, name, Some(1))
    val e4 = MultiEntity(nextId, name, None)
    run(for {
      _ <- index.create
      _ <- multiTable.insertBatch(e1,e2,e3,e4)
      results <- index(MultiIndexKey(Some(1), name))
    } yield {
      assert(results == Set(e1,e3))
    })
  }

  test("An index doesn't compile unless the fields are subset of the table") {
    assertDoesNotCompile(
      "val index2: Index[MultiIndexKeyNotQuiteRight,Id,MultiEntity] = Index(multiTable)"
    )
  }

  test("Query by Index with no results returns an empty set") {
    val index: Index[MultiIndexKey,Id,MultiEntity] = Index(multiTable)
    assert(run(index(MultiIndexKey(Some(0), randomString))) == Set())
  }

  test("An index can be created and used on a field of a nested object") {
    val index: Index[NestedIndexKey,Id,NestedEntity] = Index(nestedTable)
    run(index.create)
  }

  test("compile time checking on index with with a nested object key") {
    assertDoesNotCompile(
      "val index: Index[NotQuiteRightNestedIndexKey,Id,NestedEntity] = Index(nestedTable)"
    )
  }

}

case class UniqueIndexEntity(id: Id, name: String)
case class UniqueKey(name: String)
case class WrongUniqueKey(nme: String)

case class TestUniqueIndex(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val uniqueTable = Table[Id,UniqueIndexEntity]("uniqueTable")
  override val allTables = Seq(uniqueTable)
  val index: UniqueIndex[UniqueKey,Id,UniqueIndexEntity] = UniqueIndex(uniqueTable)
  

  test("A unique index enforces uniqueness") {
    run(index.create)
    val name = randomString
    run(uniqueTable.insert(UniqueIndexEntity(nextId, name)))
    val violation = uniqueTable.insert(UniqueIndexEntity(nextId, name))
    if (dialect == Postgresql) {
      assertThrows[Exception] { 
        run(violation)
      }
    } else {
      assertThrows[java.sql.SQLIntegrityConstraintViolationException] {
        run(violation)
      }
    }
  }

  test("Query by unique Index") {
    val e1 = UniqueIndexEntity(nextId, randomString)
    val e2 = UniqueIndexEntity(nextId, randomString)
    run(uniqueTable.insertBatch(e1,e2))
    assert(run(index(UniqueKey(e2.name))) == Some(e2))
    assert(run(index(UniqueKey(e1.name))) == Some(e1))
  }

  test("A unique index doesn't compile unless the fields are subset of the table") {
    assertDoesNotCompile(
      "val index: UniqueIndex[UniqueKeyNotQuiteRight,Id,UniqueIndexEntity] = UniqueIndex(uniqueTable)"
    )
  }

  test("Query by unique Index with no results returns None") {
    assert(run(index(UniqueKey(randomString))) == None)
  }
}


case class Parent(id: Id, name: String)
case class Child(id: Id, parentId: Id, x: Int)
case class ChildToParent(parentId: Id)

case class OptChild(id: Id, parentId: Option[Id], x: Int)
case class OptChildToParent(parentId: Option[Id])

case class ForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[Id,Parent]("parent")
  val childTable = Table[Id,Child]("child")
  val optChildTable = Table[Id,OptChild]("optChild")
  override val allTables = Seq(parentTable,childTable,optChildTable)
  implicit val flattenedChildToParent = Flattened[ChildToParent, Int::HNil]
  val foreignKey = ForeignKey(childTable, parentTable, classOf[ChildToParent])

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("Query by Foreign Key") {
    val parent1 = Parent(nextId, "parent1")
    val childOf1a = Child(nextId, parent1.id, 1)
    val childOf1b = Child(nextId, parent1.id, 2)
    val parent2 = Parent(nextId, "parent2")
    val childOf2 = Child(nextId, parent2.id, 3)
    val parent3 = Parent(nextId, "parent3")
    run(for {
      _ <- parentTable.insertBatch(parent1, parent2, parent3)
      _ <- childTable.insertBatch(childOf1a, childOf1b, childOf2)
      childrenOf1 <- foreignKey.index(parent1.id)
      childrenOf2 <- foreignKey.index(parent2.id)
      childrenOf3 <- foreignKey.index(parent3.id)
    } yield {
      assert(childrenOf1 == Set(childOf1a, childOf1b))
      assert(childrenOf2 == Set(childOf2))
      assert(childrenOf3 == Set())
    })
  }

  test("A foreign key is a constraint") {
    run(foreignKey.create)
    assertThrows[Exception] { run(childTable.insert(Child(nextId, nextId, 10))) }
  }

  test("An optional foreign key") {
    val parent = Parent(nextId, "parent1")
    val childOfSome = OptChild(nextId, Some(parent.id),1)
    val childOfNone = OptChild(nextId, None, 2)
    implicit val flattenedOptChildToParent = Flattened[OptChildToParent, Int::HNil]
    val fkey = ForeignKey(optChildTable, parentTable, classOf[OptChildToParent])
    run(for {
      _ <- parentTable.insert(parent)
      _ <- optChildTable.insertBatch(childOfSome, childOfNone)
      children <- fkey.index(parent.id)
    } yield {
      assert(children == Set(childOfSome))
    })
  }
}

case class ParentOfNested(id: Id, name: String)
case class NestedId(parentId: Id, x: Int)
case class NestedNestedId(parentId: Id)
case class NestedToParent(nested: NestedNestedId)
case class NestedChild(id: Id, nested: NestedId, name: String)

case class NestedForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[Id,ParentOfNested]("parentOfNested")
  val childTable = Table[Id,NestedChild]("nestedChild")
  override val allTables = Seq(parentTable,childTable)
  implicit val flattenedChildToParent = Flattened[NestedToParent, Int::HNil]
  val foreignKey = ForeignKey(childTable, parentTable, classOf[NestedToParent])

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("A foreign key can be a field of a nested object") {
    val parent1 = ParentOfNested(nextId, "parent1")
    val child1 = NestedChild(nextId, NestedId(parent1.id, 1), "1")
    val parent2 = ParentOfNested(nextId, "parent2")
    val child2 = NestedChild(nextId, NestedId(parent2.id, 2), "2")
    val parent3 = Parent(nextId, "parent3")
    run(for {
      _ <- parentTable.insertBatch(parent1, parent2)
      _ <- childTable.insertBatch(child1, child2)
      childrenOf1 <- foreignKey.index(parent1.id)
      childrenOf2 <- foreignKey.index(parent2.id)
    } yield {
      assert(childrenOf1 == Set(child1))
      assert(childrenOf2 == Set(child2))
    })
  }
}

case class CompositeId(x: Int, y: Int)
case class CompositeParent(id: CompositeId, name: String)
case class CompositeChild(id: Id, y: Int)
case class CompositeComponent(id: Int)
case class CompositeToParent(id: CompositeComponent, y: Int)


case class CompositeForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[CompositeId,CompositeParent]("compositeParent")
  val childTable = Table[Id,CompositeChild]("compositeChild")
  override val allTables = Seq(parentTable,childTable)
  implicit val flattenedChildToParent = Flattened[CompositeToParent, Int::Int::HNil]
  val foreignKey = ForeignKey(childTable, parentTable, classOf[CompositeToParent])

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("Composite foreign key can be a field of a nested object") {
    val parent1 = CompositeParent(CompositeId(1,2), "parent1")
    val parent2 = CompositeParent(CompositeId(2,3), "parent2")
    val child1 = CompositeChild(Id(parent1.id.x), parent1.id.y)
    val child2 = CompositeChild(Id(parent2.id.x), parent2.id.y)
    val parent3 = CompositeParent(CompositeId(3,4), "parent3")
    run(for {
      _ <- parentTable.insertBatch(parent1, parent2, parent3)
      _ <- childTable.insertBatch(child1, child2)
      childrenOf1 <- foreignKey.index(parent1.id)
      childrenOf2 <- foreignKey.index(parent2.id)
    } yield {
      assert(childrenOf1 == Set(child1))
      assert(childrenOf2 == Set(child2))
    })
  }

  test("A foreign key must be a subset of the from table") (pending)

  test("A foreign key must line up with the primary key") (pending)
}


class PendingTests extends FunSuite {

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

  test("Query a View") (pending)

  test("Join with a View") (pending)

  test("field name overrides work") (pending)

  test("sql type overrides work") (pending)

  test("select by in clause (new feature)") (pending)

  //creating an Autogen with a non-integral primary key shouldn't compile
  //Primitive primary key is called "id"
  //Name of primitive primary key can be overridden
  //uuid primary key
  //Select by sets of key values
  //Naming conversions
  //tuples to Case Classes for index queries
  //error messags on implicit failures?
  //Define foreign keys and indexes by explicitly passing column names
}
