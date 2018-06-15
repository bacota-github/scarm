package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class AutogenEntity(id: Id, name: String)

case class TestAutogen(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val autogenTable = Autogen[Id,AutogenEntity]("autogen")
  override val  allTables = Seq(autogenTable)

  test("insertReturningKey of an entity with autogen primary key returns the correct Key and the entity can be selected") {
    val e = AutogenEntity(Id(0), randomString)
    run(for {
      k <- autogenTable.insertReturningKey(e)
      eNew <- autogenTable(k)
    } yield {
      assert(eNew == Some(e.copy(id=k)))
    })
  }


  test("insertAllReturningKey on entities with autogen primary key returns the correct Keys and the entities can be selected") {
    val e1 = AutogenEntity(Id(0), randomString)
    val e2 = AutogenEntity(Id(0), randomString)
    val e3 = AutogenEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(autogenTable.insertAllReturningKeys(e1,e2,e3))
    val zipped = keys zip entities
    for ((k,e) <- zipped) {
      val readName = run(autogenTable(k)).get.name
      assert(e.name == readName)
    }
  }

  test("insertReturning an entity with autogen primary key returns the entity and the entity can be selected") {
    val e = AutogenEntity(Id(0), randomString)
    run(for {
      returned <- autogenTable.insertReturning(e)
      selected <- autogenTable(returned.id)
    } yield {
      assert(returned.name == e.name)
      assert(selected.get.name == e.name)
    })
  }

  test("insertAllReturning entities with autogen primary key returns the entities and the entities can be selected") {
    val e1 = AutogenEntity(Id(0), randomString)
    val e2 = AutogenEntity(Id(0), randomString)
    val e3 = AutogenEntity(Id(0), randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(autogenTable.insertAllReturning(e1,e2,e3))
    val zipped = returned zip entities
    for ((ret, e) <- zipped) {
      assert(ret.name == e.name)
      assert(run(autogenTable(ret.id)).get.name == e.name)
    }
  }

  test("after deleting by autogen primary key, selecting on those keys returns None") {
    val e1 = AutogenEntity(Id(0), randomString)
    val e2 = AutogenEntity(Id(0), randomString)
    val e3 = AutogenEntity(Id(0), randomString)
    val keys = run(autogenTable.insertAllReturningKeys(e1,e2,e3))
    assert(run(autogenTable.delete(keys(0), keys(1))) == 2)
    assert(run(autogenTable(keys(0))) == None)
    assert(run(autogenTable(keys(1))) == None)
    //sneak in a test for accidental deletion
    assert(run(autogenTable(keys(2))).get.name == e3.name)
  }

  test("updates of entities with autogen primary key are reflected in future selects") {
    val entities = Seq(
      AutogenEntity(Id(0), randomString),
      AutogenEntity(Id(0), randomString),
      AutogenEntity(Id(0), randomString)
    )
    val Seq(e1,e2,e3) = run(autogenTable.insertAllReturning(entities:_*))
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
