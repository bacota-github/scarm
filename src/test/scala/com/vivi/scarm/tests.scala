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

case class AnyRefKeyEntity(id: String, name: String) extends Entity[String]

case class PrimitiveKeyEntity(id: Long, name: String) extends Entity[Long]

//just verifying that the boolean type can be created
case class EntityWithBoolean(id: Long, name: String, maybe: Boolean)
    extends Entity[Long]

case class Wrapped(id: Int) extends AnyVal
case class WrappedKeyEntity(id: Wrapped, name: String) extends Entity[Wrapped]

case class InnerKey(x: Short, y: Short)
case class CompositeKey(first: Long, inner: InnerKey, last: String)
case class CompositeKeyEntity(id: CompositeKey, name: String)
    extends Entity[CompositeKey]

case class DateEntity(id: Int,
  instant: Instant = Instant.now,
  localDate: LocalDate = LocalDate.now,
  localDateTime: LocalDateTime = LocalDateTime.now,
  localTimex: LocalTime = LocalTime.now
) extends Entity[Int]

case class Level2(x: Wrapped, y: String)
case class Level1(x: Int, y: Int, level2: Level2)
case class DeeplyNestedEntity(id: Wrapped, x: Int, nested: Level1)
    extends Entity[Wrapped]

case class NullableEntity(id: Wrapped, name: Option[String])
    extends Entity[Wrapped]

case class NullableNestedEntity(id: Wrapped, nested: Option[Level1])
    extends Entity[Wrapped]

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

  val primitiveTable = Table[Long,PrimitiveKeyEntity]("primitive")
  val booleanTable = Table[Long,EntityWithBoolean]("boolean")
  val anyRefTable = Table[String,AnyRefKeyEntity]("string")
  val wrappedTable = Table[Wrapped,WrappedKeyEntity]("wrapped")
  val compositeTable = Table[CompositeKey,CompositeKeyEntity]("composite")
  val dateTable = Table[Int,DateEntity]("date")
  val nestedTable = Table[Wrapped,DeeplyNestedEntity]("nested")
  val nullableTable = Table[Wrapped,NullableEntity]("nullable")
  val nullableNestedTable = Table[Wrapped,NullableNestedEntity]("nullable_nested")

  val allTables = Seq(primitiveTable, booleanTable, anyRefTable, wrappedTable,
    compositeTable, dateTable, nestedTable, nullableTable, nullableNestedTable)

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

  private def randomString: String = java.util.UUID.randomUUID().toString

  test("After inserting an entity into a table with AnyRef primary key, the entity can be selected")  {
    val e1 = AnyRefKeyEntity(randomString, randomString)
    val e2 = AnyRefKeyEntity(randomString, randomString)
    val e3 = AnyRefKeyEntity(randomString, randomString)
    for {
      i1 <- anyRefTable.insert(e1)
      i2 <- anyRefTable.insert(e2)
      i3 <- anyRefTable.insert(e3)
      e2New <- anyRefTable(e2.id)
      e1New <- anyRefTable(e1.id)
      e3New <- anyRefTable(e3.id)
    } yield {
      assert (i1 == 1)
      assert (i2 == 1)
      assert (i3 == 1)
      assert(e1New == e1)
      assert(e2New == e2)
      assert(e3New == e3)
    }
  }

  test("After inserting a batch of entities into a table with AnyRef primary key, every entity can be selected") (pending)
  test("insertReturningKey of an entity with AnyRef primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with AnyRef primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with AnyRef primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with AnyRef primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by AnyRef primary, selecting on those keys returns None") (pending)
  test("entities with AnyRef primary are not accidentally deleted") (pending)
  test("updates of entities with AnyRef primary key are reflected in future selects") (pending)
  test("entities with AnyRef primary key are not accidentally updated") (pending)
  test("AnyRef primary key enforces uniqueness") (pending)

  test("After inserting an entity into a table with primitive primary key, the entity can be selected") (pending)
  test("After inserting a batch of entities into a table with primitive primary key, every entity can be selected") (pending)
  test("insertReturningKey of an entity with primitive primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with primitive primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with primitive primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with primitive primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by primitive primary, selecting on those keys returns None") (pending)
  test("entities with primitive primary are not accidentally deleted") (pending)
  test("updates of entities with primitive primary key are reflected in future selects") (pending)
  test("entities with primitive primary key are not accidentally updated") (pending)
  test("primitive primary key enforces uniqueness") (pending)

  test("After inserting an entity into a table with user defined AnyVal primary key, the entity can be selected") (pending)
  test("After inserting a batch of entities into a table with user defined AnyVal primary key, every entity can be selected") (pending)
  test("insertReturningKey of an entity with user defined AnyVal primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with user defined AnyVal primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with user defined AnyVal primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with user defined AnyVal primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by user defined AnyVal primary, selecting on those keys returns None") (pending)
  test("entities with user defined AnyVal primary are not accidentally deleted") (pending)
  test("updates of entities with user defined AnyVal primary key are reflected in future selects") (pending)
  test("entities with user defined AnyVal primary key are not accidentally updated") (pending)
  test("user defined AnyVal primary key enforces uniqueness") (pending)

  test("After inserting an entity into a table with composite primary key, the entity can be selected") (pending)
  test("After inserting a batch of entities into a table with composite primary key, every entity can be selected") (pending)
  test("insertReturningKey of an entity with composite primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with composite primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with composite primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with composite primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by composite primary, selecting on those keys returns None") (pending)
  test("entities with composite primary are not accidentally deleted") (pending)
  test("updates of entities with composite primary key are reflected in future selects") (pending)
  test("entities with composite primary key are not accidentally updated") (pending)
  test("composite primary key enforces uniqueness") (pending)


  test("insertReturningKey of an entity with autogen primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with autogen primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with autogen primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with autogen primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by autogen primary, selecting on those keys returns None") (pending)
  test("entities with autogen primary are not accidentally deleted") (pending)
  test("updates of entities with autogen primary key are reflected in future selects") (pending)
  test("entities with autogen primary key are not accidentally updated") (pending)

  test("insertReturningKey of an entity with wrapped autogen primary key returns the correct Key and the entity can be selected") (pending)
  test("insertBatchReturningKey on entities with wrapped autogen primary key returns the correct Keys and the entities can be selected") (pending)
  test("insertReturning an entity with wrapped autogen primary key returns the entity and the entity can be selected") (pending)
  test("insertBatchReturning entities with wrapped autogen primary key returns the entities and the entities can be selected") (pending)
  test("after deleting by wrapped autogen primary, selecting on those keys returns None") (pending)
  test("entities with wrapped autogen primary are not accidentally deleted") (pending)
  test("updates of entities with wrapped autogen primary key are reflected in future selects") (pending)
  test("entities with wrapped autogen primary key are not accidentally updated") (pending)

  test("entities with java.time fields can be inserted, selected, and updated") (pending)

  test("entities with nested objects can be inserted, selected, and updated")  (pending)

  test("entities with nullable (Option) fields can inserted, selected, and updated")   (pending)

  test("entities with nullable (Option) nested fields can inserted, selected, and updated")   (pending)


/*
  test("after inserting entities, they can be selected by primary key") {
    val t1 = Teacher(TeacherId(0),  "entity" + System.currentTimeMillis())
    val t2 = Teacher(TeacherId(0),  "entity2")
    val op = for {
      id1 <- teachers.insertReturningKey(t1)
      id2 <- teachers.insertReturningKey(t2)
      t2Result <- teachers(id1)
      t1Result <- teachers(id2)
    } yield {
      assert(t1Result == Some(t1.copy(id=id1)))
      assert(t2Result == Some(t2.copy(id=id2)))

    }
    run(op)
  }


  test("A table scan returns all the entities in the table") {
    val table = Table[Int,IntRow]("scan_test", Seq("id"))
    try {
      val entities = Set(IntRow(1,"One"), IntRow(2,"Two"), IntRow(3, "three"))
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

  test("after deleting an entity, the entity cannot be found by primary key") {
    val t = Teacher(TeacherId(0), "A Teacher")
    val op = for {
      k <- teachers.insertReturningKey(t)
      inserted <- teachers(id)
      _ <- teachers.delete(id)
      result <- teachers(id)
    } yield {
      assert(inserted == t.copy(id = k)
      assert(result == None)
    }
    run(op)
  }

  test("only the specified entity is affected by a delete") {
    val t1 = Teacher(TeacherId(0), "Teacher 14")
    val t2 = Teacher(TeacherId(0), "Teacher 15")
    val op = for {
      id1 <- teachers.insertBatch(t1,t2)
      id2 <- teachers.delete(t1.id)
      r1 <- teachers(id1)
      r2 <- teachers(id2)
    } yield {
      assert (r1 == None)
      assert (r2 == Some(t2))
    }
  }

  test("deleting a nonexistent entity affects nothing") {
    val n = teachers.delete(TeacherId(-1))
    assert (run(n) == 0)
  }

  test("multiple entities can be deleted in one operation")  {
    val t1 = Teacher(TeacherId(0),  "entity1")
    val t2 = Teacher(TeacherId(0),  "entity2")
    val t3 = Teacher(TeacherId(0),  "entity2")
    val op = for {
      ids <- teachers.insert(t1,t2,t3)
      n <- teachers.delete(ids(0),ids(1),ids(2), TeacherId(-1))
      t1Result <- teachers(ids(0))
      t2Result <- teachers(ids(1))
      t3Result <- teachers(ids(2))
    } yield {
      assert(t1Result == None)
      assert(t2Result == None)
      assert(t3Result == None)
      assert(n == 2)
    }
    run(op)
  }

  test("after updating an entity, selecting the entity by primary key returns the new entity") {
    val t = Teacher(TeacherId(0), "A Teacher")
    val newName = "update"
    val op = for {
      inserted <- teachers.insertAndReturn(t)
      newTeacher = inserted.copy(name=newName)
      n <- teachers.update(newTeacher)
      result <- teachers(inserted.id)
    } yield {
      assert(n == 1)
      assert(result == Some(newTeacher))
    }
    run(op)
  }

  test("only the specified entity is affected by an update") {
    val t1 = Teacher(TeacherId(nextId), "A Teacher")
    val t2 = Teacher(TeacherId(nextId), "A Teacher")
    val newT = t1.copy(name="Updated")
    val op = for {
      _ <- teachers.insert(t1,t2)
      n <- teachers.update(newT)
      result <- teachers(t2.id)
    } yield {
      assert(n == 1)
      assert(result == Some(t2))
    }
    run(op)
  }

  test("updating a nonexistent entity affects nothing") {
    val t = Teacher(TeacherId(-1), "A Teacher")
    assert(0 == run(teachers.update(t)))
  }

  test("Multiple entities can be updated in one operation") {
    val t1 = Teacher(TeacherId(nextId), "A Teacher")
    val t2 = Teacher(TeacherId(nextId), "A Teacher")
    val newT1 = t1.copy(name="Updated")
    val newT2 = t2.copy(name="Updated")
    val op = for {
      _ <- teachers.insert(t1,t2)
      n <- teachers.update(newT1, newT2)
    } yield {
      assert(n == 2)
    }
    run(op)
    assert(Some(newT1) == run(teachers(t1.id)))
    assert(Some(newT2) == run(teachers(t2.id)))
  }


  test("Update doesn't compile if primary key isn't a prefix") {
    val table = Table[Int,IntRow]("update_test", Seq("id"))
    val row = IntRow(1,"One")
    assertTypeError("table.update(row)")
  }

  test("a dropped table cannot be used") { 
    val table = Table[Int,IntRow]("drop_test", Seq("id"))
    run(table.create())
    runQuietly(table.drop)
    assertThrows[SQLException] {
      run(table.insert(IntRow(1,"foo")))
    }
  }

  test("SQL on a table with a compound primary key") {
    val courseId = CourseId(nextId)
    val course = Course(courseId, "food", None)
    val teacherId = TeacherId(nextId)
    val teacher = Teacher(teacherId, "Teacher")
    val now = LocalDate.now(ZoneId.of("UTC"))
    val section1 = Section(SectionId(courseId, 1, 1), teacherId, "Room 1", 
      LocalTime.NOON, now.minusMonths(2), now.minusMonths(1))
    val section2 = Section(SectionId(courseId, 1, 2), teacherId, "Room 2",
      LocalTime.NOON, now.plusMonths(1), now.plusMonths(2))
    val newSection2 = section2.copy(room="Room X")
    val c = Meta[java.time.LocalDate]
    val op = for {
      _ <- courses.insert(course)
      _ <- teachers.insert(teacher)
      _ <- sections.insert(section1, section2)
      _ <- sections.update(newSection2)
    } yield ()
    run(op)
    assert(run(sections(section1.id)) == Some(section1))
    assert(run(sections(section2.id)) == Some(newSection2))
  }

  test("insert/select on table with all possible date fields") {
    val table = Table[Int,DateRow]("dates", Seq("id"))
    val newDate = DateRow(nextId)
    val c = Meta[LocalDateTime]
    try {
      run(table.create())
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
    }  finally {
      runQuietly(table.drop)
    }
  }

  test("SQL on a table with a String primary key") {
    val table = Table[String,StringRow]("string_test", Seq("id"))
    try {
      val e1 = StringRow("1", "One")
      val e2 = StringRow("2", "Two")
      val op = for {
        _ <- table.create()
        _ <- table.insert(e1,e2)
      } yield ()
      run(op)
      assert(run(table(e1.id)) == Some(e1))
    } finally {
      runQuietly(table.drop)
    }
  }

  test("SQL on a table with nested objects") {
    val table = Table[Int,DeeplyNested]("deeply_nested_test", Seq("id"))
    try {
      val e = DeeplyNested(nextId, 1, Level1(2, 3, Level2(4, "5")))
      run(table.create())
      run(table.insert(e))
      assert(run(table(e.id)) == Some(e))
    } finally {
      runQuietly(table.drop)
    }
  }


  test("SQL with null String field") {
    val table = Table[Int,NullableString]("nullable_string_test", Seq("id"))
    try {
      val e1 = NullableString(nextId, Some("foo"))
      val e2 = NullableString(nextId, None)
      run(table.create())
      run(table.insert(e1,e2))
      assert(run(table(e1.id)) == Some(e1))
      assert(run(table(e2.id)) == Some(e2))
    } finally {
      runQuietly(table.drop)
    }
  }


  test("SQL on a table with nullable AnyVal fields") {
    val table = Table[Int,NullableInt]("nullable_int_test", Seq("id"))
    try {
      val e1 = NullableInt(nextId, Some(1))
      val e2 = NullableInt(nextId, None)
      run(table.create())
      run(table.insert(e1,e2))
      assert(run(table(e1.id)) == Some(e1))
      assert(run(table(e2.id)) == Some(e2))
    } finally {
      runQuietly(table.drop)
    }
  }


  test("SQL on a table with nullable nested object field")  {
    val table = Table[Int,NullableNested]("nullable_nested_test", Seq("id"))
    try {
      val e1 = NullableNested(nextId, Some(Nestable(1,"1")))
      val e2 = NullableNested(nextId, None)
      run(table.create())
      run(table.insert(e1,e2))
      assert(run(table(e1.id)) == Some(e1))
      assert(run(table(e2.id)) == Some(e2))
    } finally {
      runQuietly(table.drop)
    }
  }
 */
  test("SQL on a table with autogenerated primary key") (pending)

  test("Insert ignores values for autogenerated columns") (pending)

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
