package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class ParentOfNested(id: Id, name: String)
case class NestedId(parentId: Id, x: Int)
case class NestedNestedId(parentId: Id)
case class NestedToParent(nested: NestedNestedId)
case class NestedChild(id: Id, nested: NestedId, name: String)

case class NestedForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[Id,ParentOfNested]("parentOfNested")
  val childTable = Table[Id,NestedChild]("nestedChild")
  override val allTables = Seq(parentTable,childTable)
  val foreignKey = MandatoryForeignKey(childTable, parentTable, classOf[NestedToParent])

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("A foreign key can be a field of a nested object") {
    val parent1 = ParentOfNested(nextId, "parent1")
    val child1 = NestedChild(nextId, NestedId(parent1.id, 1), "1")
    val parent2 = ParentOfNested(nextId, "parent2")
    val child2 = NestedChild(nextId, NestedId(parent2.id, 2), "2")
    val parent3 = Parent(nextId, "parent3")
    run(for {
      _ <- parentTable.insertAll(parent1, parent2)
      _ <- childTable.insertAll(child1, child2)
      childrenOf1 <- foreignKey.index(parent1.id)
      childrenOf2 <- foreignKey.index(parent2.id)
    } yield {
      assert(childrenOf1 == Set(child1))
      assert(childrenOf2 == Set(child2))
    })
  }
}
