package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class ViewEntity(id: Int, name: String, code: Int, amount: Int)
case class Amount(amount: Int)

case class TestView(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val table = Table[Int,ViewEntity]("view_table")
  override val  allTables = Seq(table)

  val view = View[(String,Int), Int](
    "select sum(amount) as amt, name, code from view_table group by name, code",
    Seq("name","code"), Seq("amt")
  )

  test("querying a view") {
    run(table.insertAll(
      ViewEntity(1,"a",1,1),
      ViewEntity(2,"a",2,2),
      ViewEntity(3,"b",1,4),
      ViewEntity(4,"a",1,3),
      ViewEntity(5,"a",2,6),
      ViewEntity(6,"b",1,9)
    ))
    assert(run(view("a",2)) == Set(8))
  }


}

