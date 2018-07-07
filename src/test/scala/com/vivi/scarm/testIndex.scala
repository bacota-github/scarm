package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class MultiEntity(id: Id, name: String, x: Option[Int])
case class MultiIndexKey(x: Option[Int], name: String) 
case class MultiIndexKeyNotQuiteRight(x: Int, nme: String)

case class NestedField(x: Int, y: String)
case class NestedEntity(id: Id, nested: NestedField)

case class NestedKey(x: Int)
case class NestedIndexKey(nested: NestedKey)
case class NotQuiteRightNestedIndexKey(nestd: NestedKey)


case class TestIndex(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val multiTable = Table[Id,MultiEntity]("multi")
  val nestedTable = Table[Id,NestedEntity]("nestedTable")

  override val allTables = Seq(multiTable,nestedTable)

  test("Query by multi-column Index") {
    val index: Index[MultiIndexKey,Id,MultiEntity] = Index(multiTable)
    val name = randomString
    val e1 = MultiEntity(nextId, name, Some(1))
    val e2 = MultiEntity(nextId, randomString, Some(2))
    val e3 = MultiEntity(nextId, name, Some(1))
    val e4 = MultiEntity(nextId, name, None)
    run(for {
      _ <- index.create
      _ <- multiTable.insert(e1,e2,e3,e4)
      results <- index(MultiIndexKey(Some(1), name))
    } yield {
      assert(results == Set(e1,e3))
    })
  }

  test("Tupled query by multi-column Index") {
    val index = Index.tupled(multiTable, classOf[MultiIndexKey])
    val name = randomString
    val e1 = MultiEntity(nextId, name, Some(1))
    val e2 = MultiEntity(nextId, randomString, Some(2))
    val e3 = MultiEntity(nextId, name, Some(1))
    val e4 = MultiEntity(nextId, name, None)
    run(for {
      _ <- multiTable.insert(e1,e2,e3,e4)
      results <- index(Some(1), name)
    } yield {
      assert(results == Set(e1,e3))
    })
  }


  test("An index doesn't compile unless the fields are subset of the table") {
    assertDoesNotCompile(
      "val index2: Index[MultiIndexKeyNotQuiteRight,Id,MultiEntity] = Index(multiTable)"
    )
  }

  test("Query by Index with no results returns an empty set") {
    val index: Index[MultiIndexKey,Id,MultiEntity] = Index(multiTable)
    assert(run(index(MultiIndexKey(Some(0), randomString))) == Set())
  }

  test("An index can be created and used on a field of a nested object") {
    val index: Index[NestedIndexKey,Id,NestedEntity] = Index(nestedTable)
    run(index.create)
  }

  test("compile time checking on index with with a nested object key") {
    assertDoesNotCompile(
      "val index: Index[NotQuiteRightNestedIndexKey,Id,NestedEntity] = Index(nestedTable)"
    )
  }

}
