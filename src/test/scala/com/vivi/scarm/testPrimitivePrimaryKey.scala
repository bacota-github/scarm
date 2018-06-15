package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class IntEntity(id: Int, name: String)

case class TestWithPrimitivePrimaryKey(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val intTable = Table[Int,IntEntity]("int_table")
  override val  allTables = Seq(intTable)

  test("After inserting an entity into a table with primitive primary key, the entity can be selected")  {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    run(for {
      i1 <- intTable.insert(e1)
      i2 <- intTable.insert(e2)
      i3 <- intTable.insert(e3)
      e2New <- intTable(e2.id)
      e1New <- intTable(e1.id)
      e3New <- intTable(e3.id)
    } yield {
      assert (i1 == 1)
      assert (i2 == 1)
      assert (i3 == 1)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("After inserting a batch of entities into a table with primitive primary key, every entity can be selected") {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    run(for {
      i <- intTable.insertBatch(e1,e2,e3)
      e2New <- intTable(e2.id)
      e1New <- intTable(e1.id)
      e3New <- intTable(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with primitive primary key returns the correct Key and the entity can be selected") {
    val e = IntEntity(nextId.id, randomString)
    run(for {
      k <- intTable.insertReturningKey(e)
      eNew <- intTable(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertAllReturningKey on entities with primitive primary key returns the correct Keys and the entities can be selected") {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(intTable.insertAllReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(intTable(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with primitive primary key returns the entity and the entity can be selected") {
    val e = IntEntity(nextId.id, randomString)
    run(for {
      returned <- intTable.insertReturning(e)
      selected <- intTable(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertAllReturning entities with primitive primary key returns the entities and the entities can be selected") {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(intTable.insertAllReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(intTable(e.id)) == Some(e))
    }
  }

  test("after deleting by primitive primary key, selecting on those keys returns None") {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    assert(run(intTable.insertBatch(e1,e2,e3)) == 3)
    assert(run(intTable.delete(e1.id,e2.id)) == 2)
    assert(run(intTable(e1.id)) == None)
    assert(run(intTable(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(intTable(e3.id)) == Some(e3))
  }

  test("updates of entities with primitive primary key are reflected in future selects") {
    val e1 = IntEntity(nextId.id, randomString)
    val e2 = IntEntity(nextId.id, randomString)
    val e3 = IntEntity(nextId.id, randomString)
    assert(run(intTable.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(intTable.update(update1, update2)) == 2)
    assert(run(intTable(e1.id)) == Some(update1))
    assert(run(intTable(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(intTable(e3.id)) == Some(e3))
  }
}

