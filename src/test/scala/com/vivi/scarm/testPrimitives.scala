package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._



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
