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

case class ChildKey(name: String)

case class Grandchild(id: Id, parentId: Id, name: String)
case class GrandchildToParent(parentId: Id)

case class WrongKeyName(parent: Id)
case class WrongKeyType(name: String)
case class OptionalChildToParent(parentId: Option[Id])

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
  val grandchildTable = Table[Id,Grandchild]("grandchild")
  override val allTables = Seq(parentTable,childTable,optChildTable,grandchildTable)
  val foreignKey = MandatoryForeignKey(childTable, parentTable, classOf[ChildToParent])
  val grandFKey = MandatoryForeignKey(grandchildTable, childTable, classOf[GrandchildToParent])

  val parent1 = Parent(nextId, "parent1")
  val childOf1a = Child(nextId, parent1.id, 1, randomString)
  val childOf1b = Child(nextId, parent1.id, 2, randomString)
  val parent2 = Parent(nextId, "parent2")
  val childOf2 = Child(nextId, parent2.id, 3, randomString)
  val parent3 = Parent(nextId, "parent3")
  val grandchild1 = Grandchild(nextId, childOf1b.id, randomString)
  val grandchild2 = Grandchild(nextId, childOf1b.id, randomString)
  val grandchild3 = Grandchild(nextId, childOf2.id, randomString)

  val parents = Seq(parent1, parent2, parent3)
  val children = Seq(childOf1a, childOf1b, childOf2)
  val grandchildren = Seq(grandchild1, grandchild2, grandchild3)

  override def beforeAll() {
    super.beforeAll()
    run(parentTable.insertBatch(parents:_*))
    run(childTable.insertBatch(children:_*))
    run(grandchildTable.insertBatch(grandchildren:_*))
  }

  override def afterAll() {
    run(childTable.drop)
    super.afterAll()
  }

  test("Query by foreign key") {
    assert(run(foreignKey.fetchBy(parent1.id)) == Set(childOf1a, childOf1b))
    assert(run(foreignKey.fetchBy(parent2.id)) == Set(childOf2))
    assert(run(foreignKey.fetchBy(parent3.id)) == Set())
  }

  test("Many-to-one join on foreign key")  {
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val parent = run(parentTable(child.parentId)).get
      val query = childTable :: foreignKey.manyToOne
      val result = run(query(child.id))
      assert(result == Some((child, parent)))
    }
  }

  test("One-to-many join on foreign key")  {
    for (p <- parents) {
      val parent = run(parentTable(p.id)).get
      val children = run(foreignKey.fetchBy(parent.id))
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
    val fkey = OptionalForeignKey(optChildTable, parentTable, classOf[OptChildToParent])
    run(for {
      _ <- parentTable.insert(parent)
      _ <- optChildTable.insertBatch(childOfSome, childOfNone)
      children <- fkey.fetchBy(parent.id)
    } yield {
      assert(children == Set(childOfSome))
    })

    val join = optChildTable :: fkey.manyToOne
    assert(run(join(childOfSome.id)) == Some(childOfSome, Some(parent)))
    assert(run(join(childOfNone.id)) == Some(childOfNone, None))
  }

  test("A foreign key must be a subset of the from table") {
    assertDoesNotCompile(
      "val badKey = MandatoryForeignKey(childTable, parentTable, classOf[WrongKeyName])"
    )
  }

  test("A foreign key must line up with the primary key") {
    assertDoesNotCompile(
      "val badKey = MandatoryForeignKey(childTable, parentTable, classOf[WrongKeyType])"
    )
  }

  test("A mandatory foreign key must line up exactly with the primary key") {
    assertDoesNotCompile(
      "val badKey = MandatoryForeignKey(childTable, parentTable, classOf[OptionalChildToParent])"
    )
  }

  test("Three table join by many-to-ones") {
    val query = grandchildTable :: grandFKey.manyToOne :: foreignKey.manyToOne
    for (c <- grandchildren) {
      val grandchild = run(grandchildTable(c.id)).get
      val child = run(childTable(grandchild.parentId)).get
      val parent = run(parentTable(child.parentId)).get
      val result = run(query(c.id))
      assert(result == Some((grandchild, (child, parent))))
    }
  }

  test("Three table join by one-to-many's") {
    val query = parentTable :: foreignKey.oneToMany :: grandFKey.oneToMany
    for (p <- parents) {
      val parent = run(parentTable(p.id)).get
      val children = run(foreignKey.fetchBy(parent.id))
      val descendants = children.map(child =>
        (child, run(grandFKey.fetchBy(child.id)))
      )
      val result = run(query(parent.id))
      assert(result == Some((parent, descendants)))
    }
  }

  test("Join a many-to-one and a one-to-many") {
    val query = childTable :: foreignKey.manyToOne :: foreignKey.oneToMany
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val parent = run(parentTable(child.parentId)).get
      val siblings = run(foreignKey.fetchBy(child.parentId))
      val result = run(query(child.id))
      assert(result == Some((child, (parent,siblings))))
    }
  }


  test("Join a one-to-many and a many-to-one") {
    val query = parentTable :: foreignKey.oneToMany :: foreignKey.manyToOne
    for (p <- parents) {
      val parent = run(parentTable(p.id)).get
      val children = run(foreignKey.fetchBy(parent.id))
        .map(child => (child,parent))
      val result = run(query(parent.id))
      assert(result == Some((parent, children)))
    }
  }

  test("Nested Join") {
    val query = (childTable :: foreignKey.manyToOne) ::: grandFKey.oneToMany
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val parent = run(parentTable(child.parentId)).get
      val grandchildren = run(grandFKey.fetchBy(c.id))
      val result = run(query(child.id))
      assert(result == Some((child, parent, grandchildren)))
    }
  }

  test("Join an index to a many-to-one") {
    val index: Index[ChildKey,Id,Child] = Index(childTable)
    val query = index :: foreignKey.manyToOne
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val parent = run(parentTable(child.parentId)).get
      val result = run(query(ChildKey(child.name)))
      assert(result == Set((child,parent)))
    }
  }

  test("Join an index to a one-to-many") {
    val index: Index[ChildKey,Id,Child] = Index(childTable)
    val query = index :: grandFKey.oneToMany
    for (c <- children) {
      val child = run(childTable(c.id)).get
      val grandchildren = run(grandFKey.fetchBy(child.id))
      val result = run(query(ChildKey(child.name)))
      assert(result == Set((child, grandchildren)))
    }
  }
}
