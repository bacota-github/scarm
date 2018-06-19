package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import org.scalatest._
import com.vivi.scarm._
import cats.data.NonEmptyList

case class OverrideEntity(id: Int, name1: String, name2: String)

case class TestTypeOverrides(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true ) 
) extends FunSuite with DSLTestBase  {
  val table = Table[Int,OverrideEntity]("overrides")
  override val  allTables = Seq(table)

  override def beforeAll() {
    run(table.createWithTypeOverrides(Map("name1" -> "varchar(10)")))
  }

  test("A table with type overrides can be created and used") {
    val e = OverrideEntity(1, "hi", "this is a very long column but ok here")
    run(table.insert(e))
    assert(run(table(e.id)) == Some(e))
  }

  test("A table with type overrides uses the overridden types") {
    val e = OverrideEntity(1, "this column is way too long", "there")
    assertThrows[Exception] {
      run(table.insert(e))
    }
  }
}
