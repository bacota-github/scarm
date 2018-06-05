package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

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
