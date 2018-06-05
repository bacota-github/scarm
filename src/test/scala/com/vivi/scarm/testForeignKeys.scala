package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest._

import com.vivi.scarm._

case class Parent(id: Id, name: String)
case class Child(id: Id, parentId: Id, x: Int, name: String)
case class ChildToParent(parentId: Id)

case class WrongKeyName(parent: Id)
case class WrongKeyType(name: String)

case class OptChild(id: Id, parentId: Option[Id], x: Int)
case class OptChildToParent(parentId: Option[Id])

case class ForeignKeyTests(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {

  val parentTable = Table[Id,Parent]("parent")
  val childTable = Table[Id,Child]("child")
  val optChildTable = Table[Id,OptChild]("optChild")
  override val allTables = Seq(parentTable,childTable,optChildTable)
  val foreignKey = ForeignKey(childTable, parentTable, classOf[ChildToParent])

  val parent1 = Parent(nextId, "parent1")
  val childOf1a = Child(nextId, parent1.id, 1, randomString)
  val childOf1b = Child(nextId, parent1.id, 2, randomString)
  val parent2 = Parent(nextId, "parent2")
  val childOf2 = Child(nextId, parent2.id, 3, randomString)
  val parent3 = Parent(nextId, "parent3")

  val children = Seq(childOf1a, childOf1b, childOf2)
  val parents = Seq(parent1, parent2, parent3)

  override def beforeAll() {
    super.beforeAll()
    run(parentTable.insertBatch(parents:_*))
    run(childTable.insertBatch(children:_*))
  }

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("Query by foreign key") {
    assert(run(foreignKey.index(parent1.id)) == Set(childOf1a, childOf1b))
    assert(run(foreignKey.index(parent2.id)) == Set(childOf2))
    assert(run(foreignKey.index(parent3.id)) == Set())
  }

  test("Many-to-one join on foreign key")  {
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val parent = run(parentTable(child.parentId)).get
      val query = childTable :: foreignKey.manyToOne
      val result = run(query(child.id))
      assert(result == Some((child, Some(parent))))
    }
  }

  test("One-to-many join on foreign key")  {
    for (p <- parents) {
      val parent = run(parentTable(p.id)).get
      val children = run(foreignKey.index(parent.id))
      val query = parentTable :: foreignKey.oneToMany
      val result = run(query(parent.id))
      assert(result == Some((parent, children)))
    }
  }

  test("Many-to-one joins in the wrong direction don't compile") {
    assertDoesNotCompile(
      "val query = parentTable :: foreignKey.manyToOne"
    )
  }

  test("One-to-Many joins in the wrong direction don't compile") {
    assertDoesNotCompile(
      "val query = childTable :: foreignKey.oneToMany"
    )
  }

  test("A foreign key is a constraint") {
    run(foreignKey.create)
    assertThrows[Exception] {
      run(childTable.insert(Child(nextId, nextId, 10, randomString)))
    }
  }

  test("An optional foreign key") {
    val parent = Parent(nextId, "parent1")
    val childOfSome = OptChild(nextId, Some(parent.id),1)
    val childOfNone = OptChild(nextId, None, 2)
    val fkey = ForeignKey(optChildTable, parentTable, classOf[OptChildToParent])
    run(for {
      _ <- parentTable.insert(parent)
      _ <- optChildTable.insertBatch(childOfSome, childOfNone)
      children <- fkey.index(parent.id)
    } yield {
      assert(children == Set(childOfSome))
    })

    val join = optChildTable :: fkey.manyToOne
    assert(run(join(childOfSome.id)) == Some(childOfSome, Some(parent)))
    assert(run(join(childOfNone.id)) == Some(childOfNone, None))
  }

  test("A foreign key must be a subset of the from table") {
    assertDoesNotCompile(
      "val badKey = ForeignKey(childTable, parentTable, classOf[WrongKeyName])"
    )
  }

  test("A foreign key must line up with the primary key") {
    assertDoesNotCompile(
      "val badKey = ForeignKey(childTable, parentTable, classOf[WrongKeyType])"
    )
  }
}
