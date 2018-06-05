package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._


case class CompositeId(x: Int, y: Int)
case class CompositeParent(id: CompositeId, name: String)
case class CompositeChildId(parentId: CompositeId, c: Int)
case class CompositeChild(id: CompositeChildId, name: String)
case class CompositeComponent(parentId: CompositeId)
case class CompositeToParent(id: CompositeComponent)


case class CompositeForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[CompositeId,CompositeParent]("compositeParent")
  val childTable = Table[CompositeChildId,CompositeChild]("compositeChild")
  override val allTables = Seq(parentTable,childTable)
  val foreignKey = ForeignKey(childTable, parentTable, classOf[CompositeToParent])

  val parent1 = CompositeParent(CompositeId(1,2), "parent1")
  val parent2 = CompositeParent(CompositeId(2,3), "parent2")
  val child1 = CompositeChild(CompositeChildId(parent1.id, 1), randomString)
  val child2 = CompositeChild(CompositeChildId(parent2.id, 1), randomString)
  val parent3 = CompositeParent(CompositeId(3,4), "parent3")

  val parents = Seq(parent1, parent2, parent3)
  val children = Seq(child1,child2)

  override def beforeAll() {
    super.beforeAll()
    run(parentTable.insertBatch(parents:_*))
    run(childTable.insertBatch(children:_*))
  }

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("Composite foreign key can be a field of a nested object") {
    run(for {
      childrenOf1 <- foreignKey.index(parent1.id)
      childrenOf2 <- foreignKey.index(parent2.id)
    } yield {
      assert(childrenOf1 == Set(child1))
      assert(childrenOf2 == Set(child2))
    })
  }

  test("Many-to-one join on composite foreign key")  {
    for (c <- children) {
    val child = run(childTable(c.id)).get
    val parent = run(parentTable(child.id.parentId)).get
      val query = childTable :: foreignKey.manyToOne
      val result = run(query(child.id))
      assert(result == Some((child, Some(parent))))
    }
  }

  test("One-to-many join on composite foreign key")  {
    for (p <- parents) {
      val parent = run(parentTable(p.id)).get
      val children = run(foreignKey.index(parent.id))
      val query = parentTable :: foreignKey.oneToMany
      val result = run(query(parent.id))
      assert(result == Some((parent, children)))
    }
  }
}
