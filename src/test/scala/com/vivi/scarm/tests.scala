package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.sql.{ SQLException }
import org.scalatest._

import com.vivi.scarm._
import com.vivi.scarm.FieldMap._

import TestObjects._

object DSLSuite {
  val hsqldbCleanup = (xa:Transactor[IO]) => {
    val op = for { 
      _ <- sql"""DROP SCHEMA PUBLIC CASCADE""".update.run
      _ <- sql"""SHUTDOWN IMMEDIATELY""".update.run
    } yield false
    op.transact(xa).unsafeRunSync()
  }
}

class DSLSuite extends Suites(
  new DSLTest("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:file:testdb",
    "SA", "", DSLSuite.hsqldbCleanup),
  new DSLTest("org.postgresql.Driver", "jdbc:postgresql:scarm", "scarm", "scarm"),
  new DSLTest("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/scarm?serverTimezone=UTC&useSSL=false", "scarm", "scarm")
)

class DSLTest(driver: String,
  url: String,
  username: String,
  pass: String,
  cleanup: (Transactor[IO] => Boolean) = (_ => true ) 
) extends FunSuite with BeforeAndAfterAll {

  implicit val xa = Transactor.fromDriverManager[IO](driver, url, username, pass)

  private def run[T](op: ConnectionIO[T]): T = op.transact(xa).unsafeRunSync()

  private def createAll() = 
    for (t <- allTables) {
      t.create().transact(xa).unsafeRunSync()
    }

  private def dropAll(xa: Transactor[IO]): Unit = 
    for (t <-allTables) {
      try {
        t.dropCascade.transact(xa).unsafeRunSync()
      } catch { case e: Exception =>
          info(s"failed dropping ${t.name} ${e.getMessage()}")
      }
    }

  override def beforeAll() {
    createAll()
  }

  override def afterAll() {
    cleanup(xa)
    if (cleanup(xa))  dropAll(xa) else ()
  }

  test("fieldLists") {
    case class Foo(sectionId: SectionId, x: Int)
    case class NestedId(sectionId: SectionId)
    case class Foo2(nestedId: NestedId, x: Int)
    println(FieldMap[(Int,Int,Int)].names)
    println(FieldMap[SectionId].names)
    println(FieldMap[(SectionId)].names)
    println(FieldMap[(SectionId, Int)].names)
    println(FieldMap[(Int,SectionId)].names)
//    println(FieldMap[Foo2].names)
  }
/*
  test("after inserting an entity, it can be selected by primary key") {
    val t1 = Teacher(TeacherId(1),  "entity1")
    val t2 = Teacher(TeacherId(2),  "entity2")
    val op = for {
      n <- teachers.insert(t1, t2)
      t2Result <- teachers(t2.id)
      t1Result <- teachers(t1.id)
    } yield {
      assert(n == 2)
      assert(t1Result == Some(t1))
      assert(t2Result == Some(t2))
    }
    run(op)
  }

  test("Multiple entities can be inserted in one operation") {
    val newTeachers = Set(Teacher(TeacherId(10),"Fred"),
      Teacher(TeacherId(11),"Barney"),
      Teacher(TeacherId(12), "Wilma")
    )
    assert(newTeachers.size == run(teachers.insert(newTeachers.toSeq: _*)))
  }

  test("A table scan returns all the entities in the table") {
    case class Row(id: Int, name: String) extends Entity[Int]
    val table = Table[Int,Row]("scan_test", Seq("id"))
    val entities = Set(Row(1,"One"), Row(2,"Two"), Row(3, "three"))
    val op = for {
      _ <- table.create()
      _ <- table.insert(entities.toSeq: _*)
      results <- table.scan(Unit)
     _ <-table.drop
    } yield {
      assert(results == entities)
    }
    run(op)
  }

  test("after deleting an entity, the entity cannot be found by primary key") {
    val t = Teacher(TeacherId(3), "A Teacher")
    val op = for {
      _ <- teachers.insert(t)
      _ <- teachers.delete(t.id)
      result <- teachers(t.id)
    } yield {
      assert(result == None)
    }
    run(op)
  }

  test("only the specified entity is affected by a delete") {
    val t1 = Teacher(TeacherId(14), "Teacher 14")
    val t2 = Teacher(TeacherId(15), "Teacher 15")
    val op = for {
      _ <- teachers.insert(t1,t2)
      _ <- teachers.delete(t1.id)
      r1 <- teachers(t1.id)
      r2 <- teachers(t2.id)
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
    val t1 = Teacher(TeacherId(16),  "entity1")
    val t2 = Teacher(TeacherId(17),  "entity2")
    val op = for {
      _ <- teachers.insert(t1,t2)
      n <- teachers.delete(t1.id,t2.id, TeacherId(-1))
      t1Result <- teachers(t1.id)
      t2Result <- teachers(t2.id)
    } yield {
      assert(n == 2)
      assert(t1Result == None)
      assert(t2Result == None)
    }
    run(op)
  }

  test("after updating an entity, selecting the entity by primary key returns the new entity") {
    val t = Teacher(TeacherId(4), "A Teacher")
    val newT = t.copy(name="Updated")
    val op = for {
      _ <- teachers.insert(t)
      n <- teachers.update(newT)
      result <- teachers(t.id)
    } yield {
      assert(n == 1)
      assert(result == Some(newT))
    }
    run(op)
  }

  test("only the specified entity is affected by an update") {
    val t1 = Teacher(TeacherId(18), "A Teacher")
    val t2 = Teacher(TeacherId(19), "A Teacher")
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
    val t1 = Teacher(TeacherId(21), "A Teacher")
    val t2 = Teacher(TeacherId(22), "A Teacher")
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
    case class Row(name: String, id: Int) extends Entity[Int]
    val table = Table[Int,Row]("update_test", Seq("id"))
    val row = Row("One",1)
    assertTypeError("table.update(row)")
  }

  test("a dropped table cannot be used") { 
    case class Row(name: String, id: Int) extends Entity[Int]
    val table = Table[Int,Row]("drop_test", Seq("id"))
    run(table.create())
    run(table.drop)
    assertThrows[SQLException] {
      run(table.insert(Row("foo", 1)))
    }
  }

  test("SQL on a table with date fields") (pending)

  test("SQL on a table with a primitive primary key") (pending)

  test("SQL on a table with a String primary key") (pending)

  test("SQL on a table with a compound primary key") (pending)

  test("SQL on a table with a dates in primary key") (pending)

  test("SQL on a table with nested objects") (pending)

  test("SQL on a table with a primary key containing nested object") (pending)

  test("SQL on a table with nullable String fields") (pending)

  test("SQL on a table with nullable AnyVal fields") (pending)

  test("SQL on a table with nullable nested object field") (pending)

  test("SQL on a table with autogenerated primary key") (pending)

  test("SQL on a table with explicit field names") (pending)

  test("SQL on a table with explicit key names") (pending)

  test("SQL on a table with field overrides") (pending)

  test("SQL on a table with sql type overrides") (pending)

  test("Query by Index") (pending)

  test("Query by Index with no results") (pending)

  test("Query by Unique Index") (pending)

  test("Query by Unique Index with no results") (pending)

  test("Query by Foreign Key") (pending)

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
 */
}
