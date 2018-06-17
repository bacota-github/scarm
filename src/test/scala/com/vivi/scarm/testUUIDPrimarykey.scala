package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._
import java.util.UUID

import com.vivi.scarm.UUIDMeta

case class Uuid(id: UUID)

case class UUIDEntity(id: Uuid, name: String)

case class TestWithUUIDPrimaryKey(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val table = Table[Uuid,UUIDEntity]("uuid_table")
  override val  allTables = Seq(table)

  def nextUuid = Uuid(UUID.randomUUID())

  test("After inserting an entity into a table with primitive primary key, the entity can be selected")  {
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    run(for {
      i1 <- table.insert(e1)
      i2 <- table.insert(e2)
      i3 <- table.insert(e3)
      e2New <- table(e2.id)
      e1New <- table(e1.id)
      e3New <- table(e3.id)
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
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    run(for {
      i <- table.insertBatch(e1,e2,e3)
      e2New <- table(e2.id)
      e1New <- table(e1.id)
      e3New <- table(e3.id)
    } yield {
      assert (i == 3)
      assert(e1New == Some(e1))
      assert(e2New == Some(e2))
      assert(e3New == Some(e3))
    })
  }

  test("insertReturningKey of an entity with primitive primary key returns the correct Key and the entity can be selected") {
    val e = UUIDEntity(nextUuid, randomString)
    run(for {
      k <- table.insertReturningKey(e)
      eNew <- table(k)
    } yield {
      assert (k == e.id)
      assert(eNew == Some(e))
    })
  }


  test("insertBatchReturningKey on entities with primitive primary key returns the correct Keys and the entities can be selected") {
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    val entities = Seq(e1,e2,e3)
    val keys = run(table.insertBatchReturningKeys(e1,e2,e3))
    assert(keys == entities.map(_.id))
    for (e <- entities) {
      assert(run(table(e.id)) == Some(e))
    }
  }

  test("insertReturning an entity with primitive primary key returns the entity and the entity can be selected") {
    val e = UUIDEntity(nextUuid, randomString)
    run(for {
      returned <- table.insertReturning(e)
      selected <- table(e.id)
    } yield {
      assert(returned == e)
      assert(selected == Some(e))
    })
  }

  test("insertBatchReturning entities with primitive primary key returns the entities and the entities can be selected") {
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    val entities = Seq(e1,e2,e3)
    val returned = run(table.insertBatchReturning(e1,e2,e3))
    assert(returned == entities)
    for (e <- entities) {
      assert(run(table(e.id)) == Some(e))
    }
  }

  test("after deleting by primitive primary key, selecting on those keys returns None") {
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    assert(run(table.insertBatch(e1,e2,e3)) == 3)
    assert(run(table.delete(e1.id,e2.id)) == 2)
    assert(run(table(e1.id)) == None)
    assert(run(table(e2.id)) == None)
    //sneak in a test for accidental deletion
    assert(run(table(e3.id)) == Some(e3))
  }

  test("updates of entities with primitive primary key are reflected in future selects") {
    val e1 = UUIDEntity(nextUuid, randomString)
    val e2 = UUIDEntity(nextUuid, randomString)
    val e3 = UUIDEntity(nextUuid, randomString)
    assert(run(table.insertBatch(e1,e2,e3)) == 3)
    val update1 = e1.copy(name=randomString)
    assert(e1 != update1)
    val update2 = e2.copy(name=randomString)
    assert(e2 != update2)
    assert(run(table.update(update1, update2)) == 2)
    assert(run(table(e1.id)) == Some(update1))
    assert(run(table(e2.id)) == Some(update2))
    //sneak in a test for accidental update
    assert(run(table(e3.id)) == Some(e3))
  }
}

