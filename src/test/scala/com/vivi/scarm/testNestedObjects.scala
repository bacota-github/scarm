package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

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
