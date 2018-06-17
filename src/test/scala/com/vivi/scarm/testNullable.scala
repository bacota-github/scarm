package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._


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
    assert(run(nullableTable.insert(e1,e2)) == 2)
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
    assert(run(nullableNestedTable.insert(e1,e2)) == 2)
    assert(run(nullableNestedTable(e1.id)) == Some(e1))
    assert(run(nullableNestedTable(e2.id)) == Some(e2))
    val updated1 = NullableNestedEntity(e1.id, None)
    val updated2 = NullableNestedEntity(e2.id, Some(Level1(2,3,Level2(4,"5"))))
    assert(run(nullableNestedTable.update(updated1, updated2)) == 2)
    assert(run(nullableNestedTable(e1.id)) == Some(updated1))
    assert(run(nullableNestedTable(e2.id)) == Some(updated2))
  }
}
