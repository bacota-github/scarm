# Scarm (Scala Relational Mapping)

Scarm is a library for using Doobie to conveniently store and retrieve case classes in a relational database.  It's goals are

1. Automatic SQL generation.
2. As much static type checking as possible.
3. Support for joins to load structures of related case classes from the database.


## Quick Overview

A long example in contained in
src/test/scala/com/vivi/demo/demo.scala.  That file also contains the
necessary imports and other setup.

Here is a quick overview

First define case classes for  a couple of entities to be stored in the database
```
case class ParentId(id: Int)
case class Parent(parent: ParentId, name: String)

case class ChildId(id: Int)
case class Child(id: ChildId, parent: ParentId, name: String, bankAccount: Int)
```
Create a couple of tables to hold entities of the requisite type.  
```
val parents = Table[ParentId,Parent]("parent")
val children = Table[ChildId,Child]("children")
```
The table definition has two type parameters -- the primary key type, and the entity type.  The primary key of the table is *always* the type of the first field in the case class.  (It should be possible to just infer the primary key type from the HList of the entity, but I haven't managed to get that working.)  

The lone argument to the table constructor is the name of the table in the database.  

SQL can be generated to create the table in the database.  

```
val createParent: ConnectionIO[Unit] = parents.create
val createChild: ConnectionIO[Unit] = children.create
```
This is useful for unit tests but in a production environment the database structures would presumably be managed by a database migration tool like flyway.

The tables will be created with the following SQL 
```
CREATE TABLE Parent(
   parent_id int,
   name varchar(255)
)
CREATE TABLE Child(
   child_id int,
   parent_id int,
   name varchar(255),
   bank_account int
)
```
The use of "_" to separate parts of nested fields (e.g. `parent_id`), and the conversion from camel case to snake case (`bankAccount` to `bank_account`) are defaults that can be changed in configuration.  The column types  can also be overridden but it's probably not worth the effort if production SQL is managed outside of the application code.

We can do DML on the tables
```
val dml: ConnectionIO[Unit] = for {
    _ <- parents.insert(Parent(ParentId(1),"Parent 1"), Parent(ParentId(2),"Parent 2"))
    _ <- children.insert(Child(ChildId(1),ParentId(1),"Child1",10), 
                         Child(ChildId(2),ParentId(1),"Child2",20), 
                         Child(ChildId(3),ParentId(2),"Child3",100)) 
    _ <- children.update(Child(ChildId(2),ParentId(1),"Child2",100)), 
    _ <- children.delete(ChildId(3))
} yield ()

```

Select from the tables by primary key 
```
val parent1: Option[Parent] = parents(ParentId(1)).transact(xa).unsafeRunSync()
```
or read all the rows from the table
```
val allParents: Set[Parent] = parents.scan(Unit).transact(xa).unsafeRunSync()
```

To define a relationship between the two tables, define a key class 
```
case class ChildToParent(parent: ParentId)
```
Then can define a relationship like this
```
val childParent = MandatoryForeignKey(children, parents, classOf[ChildToParent])
```
The ChildToParent class defines the names of the columns used in the foreign key.  This won't compile unless the fields of the key class actually exist in the Child class *and* the types of those fields line up with the Parent class's primary key.

A foreign key can be used to construct joins with the `::`` operator
```
val childrenWithParents: ConnectionIO[Option[(Child, Parent)]] =  
    (children :: childParent.manyToOne)(ChildId(1))
val parentWithChildren: ConnectionIO[Option[Parent, Set[Children])]] = 
    (parent :: childParent.oneToMany)(ParentId(1))
```

Shapeless is used to prevent foreign keys from compiling with the wrong type of index class.  For example, with a key class
```case class BadChildToParent(farent: ParentId)```
this won't compile
```
val badChildParent = MandatoryForeignKey(children, parents, classOf[BadChildToParent])
```
because `Child` does not have a field called `farent`.  

## Current Status

At the moment, Scarm has exactly one user, and that's me.  

Scarm supports Postgresql, Mysql, and Hsqldb.  I test with 9.6, 5.7, and 2.4.0 respectively.
    
