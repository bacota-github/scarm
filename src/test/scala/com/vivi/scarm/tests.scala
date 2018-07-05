package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.sql.{ SQLException }

import org.scalatest._

import com.vivi.scarm._
import com.vivi.scarm.FieldMap._

import shapeless._

object DSLSuite {
  val hsqldbCleanup = (xa:Transactor[IO]) => {
    val op = for {
      _ <- sql"DROP SCHEMA PUBLIC CASCADE".update.run
      s <- sql"SHUTDOWN IMMEDIATELY".update.run
    } yield false
    op.transact(xa).unsafeRunSync()
  }
}

case class Id(id: Int) extends AnyVal

case class IdEntity(id: Id, name: String)


class DSLSuite extends Suites(
  DSLTest("org.hsqldb.jdbc.JDBCDriver",
    "jdbc:hsqldb:file:testdb",
    "SA", "", Hsqldb,
    DSLSuite.hsqldbCleanup),

  DSLTest("org.postgresql.Driver",
    "jdbc:postgresql:scarm", "scarm", "scarm", Postgresql,
  ),

  DSLTest("com.mysql.jdbc.Driver",
    "jdbc:mysql://localhost:3306/scarm?serverTimezone=UTC&useSSL=false&sql_mode=",
    "scarm", "scarm", Mysql
  )
)

object DSLTest {
  def xa(driver: String, url: String, username: String, pass: String) =
    Transactor.fromDriverManager[IO](driver, url, username, pass)
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
  ForeignKeyTests(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  NestedForeignKeyTests(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  CompositeForeignKeyTests(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestView(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestTypeOverrides(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  TestWithNoKeyPrefix(DSLTest.xa(driver,url,username,pass),dialect,cleanup),
  demo.Demo(ScarmConfig(dialect), DSLTest.xa(driver,url,username,pass))
)


trait DSLTestBase extends Suite with BeforeAndAfterAll {
  def dialect: SqlDialect
  implicit def config = ScarmConfig(dialect, false, "_")

  def xa: Transactor[IO]
  implicit def implicitXA = xa

  def run[T](op: ConnectionIO[T]) =   op.transact(xa).unsafeRunSync()

  def allTables: Seq[Table[_,_]]
  def cleanup: (Transactor[IO] => Boolean) = (_ => true ) 

  def runQuietly(op: ConnectionIO[_]) = try {
    run(op)
  } catch { case _:Exception => }

  def createAll: Unit =  
    for (t <- allTables) {
      t.create.transact(xa).unsafeRunSync()
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


case class TestMiscellaneous(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val intTable = Table[Id,IdEntity]("misc_test")
  override val  allTables = Seq(intTable)

  test("tables with the wrong primary key should not compile") {
    val name = "badTable"
    assertDoesNotCompile(
      "val table = Table[String,IdEntity](name)"
    )
  }

  test("A table scan returns all the entities in the table") {
    val table = Table[Id,IdEntity]("scan_test")
    try {
      val entities = Set(IdEntity(nextId,randomString),
        IdEntity(nextId, randomString),
        IdEntity(nextId, randomString)
      )
      val op = for {
        _ <- table.create
        _ <- table.insert(entities.toSeq: _*)
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
    val t = IdEntity(Id(-1), randomString)
    assert(0 == run(intTable.update(t)))
  }

  test("a dropped table cannot be used") { 
    val table = Table[Id,IdEntity]("drop_test")
    run(table.create)
    runQuietly(table.drop)
    assertThrows[SQLException] {
      run(table.insert(IdEntity(Id(1),"foo")))
    }
  }
}
