# Scarm (Scala Relational Mapping)

Scarm is a library for using Doobie to conveniently store and retrieve
case classes in a relational database.  It's more like an ORM than
most Scala database libraries I've come across, but it does not use
mutable objects, lazy loading, or any of the other
unfunctional properties of JPA.

My goals with Scarm are

1. Automatic SQL generation.
2. As much static type checking as possible.
3. Support for joins to load structures of related case classes from the database.



## Quick Overview

A long example in contained in
[src/test/scala/com/vivi/demo/demo.scala](https://github.com/bacota-github/scarm/blob/master/src/test/scala/com/vivi/demo/demo.scala).  That file also contains the
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
   parent_id int primary key,
   name varchar(255) not null
)
CREATE TABLE Child(
   child_id int primary key,
   parent_id int not null,
   name varchar(255) not null,
   bank_account int not null
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
    _ <- children.update(Child(ChildId(2),ParentId(1),"Child2",100)) 
    _ <- children.delete(ChildId(3))
} yield ()

```

Select from the tables by primary key 
```
val parent1: Option[Parent] = parents(ParentId(1)).transact(xa).unsafeRunSync()
```
and read all the rows from the table
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
The `ChildToParent` class defines the names of the columns used in the foreign key.  This won't compile unless the fields of the key class actually exist in the Child class *and* the types of those fields line up with the Parent class's primary key.

A foreign key can be used to construct joins with the `::` operator
```
val childrenWithParents: ConnectionIO[Option[(Child, Parent)]] = (children :: childParent.manyToOne)(ChildId(1))
val parentWithChildren: ConnectionIO[Option[Parent, Set[Children])]] = (parent :: childParent.oneToMany)(ParentId(1))
```

Shapeless is used to prevent foreign keys from compiling with the wrong type of index class.  For example, with a key class
```case class BadChildToParent(farent: ParentId)```
this won't compile
```
val badChildParent = MandatoryForeignKey(children, parents, classOf[BadChildToParent])
```
because `Child` does not have a field called `farent`.  

## Current Status

At the moment, Scarm has exactly one user, and that's me :)

Scarm supports Postgresql, Mysql, and Hsqldb.  I test with 9.6, 5.7, and 2.4.0 r
espectively.

A Scala 2.12 package is deployed to maven central as com.vivi.scarm.


## Extended Example
This example is based on the following data model:
![Example Data Model](https://github.com/bacota-github/scarm/blob/master/demo-erd.png)

Full code is in
[src/test/scala/com/vivi/demo/demo.scala](https://github.com/bacota-github/scarm/blob/master/src/test/scala/com/vivi/demo/demo.scala). 

### Setup

These are the necessary imports for running the examples below
```
import cats.effect.IO
import doobie.ConnectionIO
import doobie.util.transactor.Transactor
import org.scalatest.{ BeforeAndAfterAll, FunSuit
e }
import com.vivi.scarm._
import java.time._

import doobie.implicits._
import shapeless._
```
Many scarm methods need a `ScarmConfig` object to define the SQL dialect and column naming conventions.  This can be passed implicitly.
```
implicit val scarmConfig = ScarmConfig(Postgresql, snakeCase=true, fieldNameSeparator="_")
```
There is also a `run` convenenience method for calling unsafeRunSync() within a transaction.  This method takes an implicit Transactor
```
implicit val xa =  Transactor.fromDriverManager[IO]("org.postgresql.Driver",
                   "jdbc:postgresql:scarm", "scarm", "scarm")
```

### A Simple Entity and Table

 A Teacher is an entity with an Int primary key.  But rather than using
 a raw Int, we wrap it in a case class because, "type all the things".

 The first field in the case class is always the primary key (and the
 entire primary key).

 The name of the primary key column will be "teacher_id", constructed
 from the names of the fields called "teacher" and "id".
```
case class TeacherId(id: Int) extends AnyVal
case class Teacher(teacher: TeacherId, name: String)
```
Table objects proxy for tables in the relational database.  
A Table's type parameters specify the type of the primary key and
of the entity stored in the table.
```
val teachers = Table[TeacherId,Teacher]("Teacher")
```
The table can be created in the database.  Actually this should be done using a migration tool like flyway, but creating from the application is useful for unit/integration tests.
      
```
val createTeachersOp: ConnectionIO[Unit] = teachers.create
run(createTeachersOp)
```
Since the primary key is always the first field of the case class,
it should be possible to use shapeless to infer the primary key
type, so we could just used
```
val teachers = Table[Teacher]("Teacher")
```
But, alas, I haven't gotten that to work.  

On the bright side, declaring the wrong primary key type results in
a compilation error "could not find implicit value for parameter primaryKey"
```
val teachers = Table[CourseId,Teacher]("Teacher")
```
Now we can run some DML and selects on the table.
```
val fred = Teacher(TeacherId(1), "Fred")
val robert = Teacher(TeacherId(2), "Robert")
val op: ConnectionIO[Unit] = for {
    //insert uses the BatchInsert API, as long as  the table isn't autogen
    numInserted <- teachers.insert(fred, robert)
    //select fred from the database.  The result is an Option type
    shouldBeFred <- teachers(fred.teacher)
    //a query for a nonexistent primary key should return None
    shouldBeNone <- teachers(TeacherId(-1))
    freddie = fred.copy(name="Freddie")
    bob = robert.copy(name="Bob")
    numUpdated <- teachers.update(freddie, bob) //uses batch update API
    shouldBeFreddie <- teachers(fred.teacher) //select fred after update
    everybody <- teachers.scan(Unit) //selects all the teachers
}  yield {
    assert(numInserted == 2)
    assert(shouldBeFred == Some(fred))
    assert(shouldBeNone == None)
    assert(numUpdated == 2)
    assert(shouldBeFreddie == Some(freddie))
    assert(everybody == Set(freddie, bob))
}
```
Deletion also works.
```
val deleteOp = for {
  nDeleted <- teachers.delete(fred.teacher, robert.teacher)
  fredFound <- teachers(fred.teacher)
} yield {
  assert(nDeleted == 2)
  assert(fredFound == None)
}
```

### Tables with Autogenerated Keys

`Autogen` creates a Table object with an autogenerated primary key
using an auto_increment field in mysql or a sequence in postgresql.

As an example, consider a case class `Course` structured similarly to `Teacher`, but there is an optional relationship from `Course` to itself, represented by the prerequisite field.
```
case class CourseId(id: Int) extends AnyVal
case class Course(course: CourseId, subject: String, prerequisite: Option[CourseId])
```
Actually the case classes should be defined outside the scope of the database objects, or some of the necessary implicit parameter passing won't work for Indexes, Foreign Keys, etc.

This code
```
val courses = Autogen[CourseId,Course]("Course")
run(courses.create)
```
Should produce a table similar to this (in MySQL syntax)
```
CREATE TABLE (
    course_id int auto_increment primary key,
    subject varchar(255) not null,
    prerequisite_id int
)
```
Note that `prerequisite_id` is nullable because the field from which it is defined is an `Option`

When we insert an entity with an autogenerated key, we usually
need to return the actual key. Note that algebra's original key is ignored.     
```
//Algebra I has no prerequisite
val algebraPrototype = Course(CourseId(0), "Algebra I", None)
val algebraId = run(courses.insertReturningKey(algebraPrototype))
val algebra = algebraPrototype.copy(course=algebraId)
val trigPrototype = Course(CourseId(0), "Trigonometry", Some(algebraId))
val trigId = run(courses.insertReturningKey(trigPrototype))
val trig = trigPrototype.copy(course=trigId)
assert(run(courses(algebraId)) == Some(algebra.copy(course=algebraId)))
assert(run(courses(trigId)) == Some(trig.copy(course=trigId)))
```

### Indexes -- Querying by Non-Primary Key Columns 

To query a table on columns other than the primary key, create an index.
The index below has three type parameters
1. The indexed key type  (TeacherByName) defines the column(s) on which to query
2. The primary key type (TeacherId) is the primary key of the table.
The primary key type is redundant, but Ihaven't been able to get rid of it.
3. The entity type (Teacher) is the type of entity stored in the table.
The index has one parameter -- a Table[TeacherId,Teacher]

Assuming this class defined for the index key
```
case class TeacherName(name: String)
```
An index object can be created 
```
val teachersByName = Index(teachers, classOf[TeacherName])
val tom1 = Teacher(TeacherId(3), "Tom")
val tom2 = Teacher(TeacherId(4), "Tom")
run(teachers.insert(tom1,tom2))
assert(run(teachersByName(TeacherName("Tom"))) == Set(tom1,tom2))
```
A `UniqueIndex` returns an `Option` instead of a `Set`. 
```
val uniqueTeacherByName = UniqueIndex(teachers, classOf[TeacherName])
run(teachers.delete(tom2.teacher))  //or an exception will be thrown by the query
assert(run(uniqueTeacherByName(TeacherName("Tom"))) == Some(tom1))
```
Through the magic of shapeless, if the fields in the key class
can't be found in the entity type, there will be compiler error
 "could not find implicit value for parameter isProjection".  So given index key classes like this
```
case class BadTeacherName(maim: String)
case class BadTeacherNameType(name: Int)
```
The following indexes creations will fail to compile.
```
val teacherByBadName = Index(teachers,classOf[BadTeacherName])
val teacherByBadlyTypedName = Index(teachers, classOf[BadTeacherNameType])
```
An index can be created in the database, though this is not necessary. 
```
run(teachersByName.create)
```
Multicolumn indexes work, too, but each field in the key class
must have a corresponding field of the same type in the entity
class, and the column names generated for those fields must
match. 

### More Complex Tables

Consider the following case classes
```
case class SectionId(course: CourseId, semester: Int, number: Int)
case class Section(section: SectionId,
  instructor: TeacherId,
  room: String,
  meetingTime: LocalTime,
  startDate: LocalDate,
  endDate: LocalDate
)

case class SectionSemester(semester: Int)
case class SectionIndex(room: String, section: SectionSemester, meetingTime: LocalTime)
```
Section has a composite primary key with fields named
section_course_id, section_semester, and section_number.


Also note the `java.time` fields, which can be stored in timestamp
columns.  By default, the corresponding database columns are expected
to use snake case, so they should be called "meeting_time",
"start_date", and "end_date", but this behavior can be changed in the
ScarmConfig object.

Then we can define the following the tables and indexes on Section:
```
val sections = Table[SectionId,Section]("Section")
val sectionByRoom = Index(sections, classOf[SectionIndex])
val roomKey = SectionIndex("12A", SectionSemester(1), LocalTime.of(14,0))
//No sections have been inserted, so an empty set is returned.
assert(run(sectionByRoom(roomKey)) == Set())
```

### Foreign Keys 

Now define one of the relationships in the data model by a foreign key.  Again, a key class must have been defined.
```
case class SectionInstructor(instructor: TeacherId)
```
This can be used to define a foreign key between section and teacher.
```
val sectionTeacher = MandatoryForeignKey(sections, teachers, classOf[SectionInstructor])
```
As with indexes, each field in the key class (`SectionInstructor`
in this case) must have a corresponding field of the same type
in the entity class, and the column names generated for those
fields must match.  

But consider the following (wrong) key classes
```
case class BadSectionInstructorName(xinstructor: TeacherId)
case class BadSectionSemester(section: SectionSemester)
```
Then these indexes fail to comiple with `could not find implicit value for parameter foreignKeyIsSubsetOfChildEntity`

   The following fails to compile with the error `could not find
implicit value for parameter foreignKeyIsSubsetOfChildEntity`
because the name of the field in `BadSectionInstructorName`
 is mispelled.
```
val badSectionTeacher1 = MandatoryForeignKey(sections, teachers, classOf[BadSectionInstructorName])
```
In addition, the types (underlying HList) of the key class must
align exactly with those of the referenced class's primary
key.   
      
The following fails to compile with the error `could not find
implicit value for parameter foreignKeyStructureMatchesPrimaryKey `
```
val badSectionTeacher2 = MandatoryForeignKey(sections, teachers, classOf[BadSectionSemester])
```
A foreign key comes with an index that can be used to query the
child table based on the primary key of the parent table. 
  ```
val sectionsTaughtByFred: Set[Section] =run(sectionTeacher.index(fred.teacher)) 
```

### Joins

The most important use of foreign keys is to construct join
queries.  The `oneToMany` property of the `sectionTeacher`
foreign key is used to construct a join from the one side
(`teachers`) to the many side (`sections`).

Given a teacher primary key, the query below returns a Teacher (if
found) with all the sections taught by the teacher (as a `Set`).
```
val teacherWithSectionsQ = teachers :: sectionTeacher.oneToMany
val fredAndHisSections: Option[(Teacher, Set[Section])] = run(teacherWithSectionsQ(fred.teacher))
```
Indexes can also be joined with. Note that the following query returns a `Set`
instead of an `Option`.  Joining with a UniqueIndex would return
an Option.
 ```
val teachersByNameWithSectionsQ = teachersByName :: sectionTeacher.oneToMany
val fredSections: Set[(Teacher, Set[Section])] = run(teachersByNameWithSectionsQ(TeacherName("Fred")))
```
Join queries are always outer joins.  In the following query, Mary is a new teacher who has not been assigned to any sections, so the query returns Mary and an empty set.
```
val mary = Teacher(TeacherId(4), "Mary")
run(for {
    _ <- teachers.insert(mary)
   maryAndHerClasses <- teacherWithSectionsQ(mary.teacher)
} yield {
   assert(maryAndHerClasses == Some((mary, Set())))
})
```
Joins also work in the manyToOne direction. The result type is a 
pair of entities, instead of an entity and a Set.
```
    val sectionWithTeachersQ = sections :: sectionTeacher.manyToOne
    val sectionAndTeacher: Option[(Section,Teacher)] = run(sectionWithTeachersQ(trigSection1.section))
```
An `OptionalForeignKey` is similar to a `MandatoryForeignKey`, but some of
the fields can be Option types even if the referenced primary key field is not.  (Of course, primary key fields are never optional).  
```
val prerequisite = OptionalForeignKey(courses, courses, classOf[CoursePrerequisite])
```
One other difference of optional foreign keys is that when used in
manyToOne joins, the result set is a pair of an entity and an
 Option entity (instead of a pair of entities)
 ```
val courseAndPrerequisiteQ = courses :: prerequisite.manyToOne
val courseAndPrerequisite: Option[(Course,Option[Course])] = run(courseAndPrerequisiteQ(trigId))
```

### Chaining Joins
Joins can be chained in any logical way with an `::` operator.

For example, given these additional case classes for  model objects and keys.
```
case class StudentId(id: Int) extends AnyVal
case class Student(id: StudentId, name: String, level: Int)

/**
  Enrollment provides a many-to-many relationship between Student and Section.
  The enrollment primary key is a composite of two foreign keys.  
  There is also an optional (nullable) grade field.
  */
case class EnrollmentId(student: StudentId, section: SectionId)
case class Enrollment(id: EnrollmentId, grade: Option[String])

case class CoursePrerequisite(prerequisite: Option[CourseId])
case class SectionCourse(section: CourseIdInSectionId)
case class CourseIdInSectionId(course: CourseId)

case class WrappedEnrollmentStudent(student: StudentId)
case class EnrollmentStudent(id: WrappedEnrollmentStudent)

case class WrappedEnrollmentSection(section: SectionId)
case class EnrollmentSection(id: WrappedEnrollmentSection)

case class SectionCount(sectionCourse: CourseId, count: Int)
```
The following tables and foreign keys can be defined
```
val students = Table[StudentId,Student]("Student")
val enrollments = Table[EnrollmentId, Enrollment]("enrollment")
val sectionCourse = MandatoryForeignKey(sections, courses, classOf[SectionCourse])
val enrollmentSection = MandatoryForeignKey(enrollments, sections, classOf[EnrollmentSection])
val enrollmentStudent = MandatoryForeignKey(enrollments, students, classOf[EnrollmentStudent])
```
To support the following join using the `::` operator.
```
val teacherAndStudentsQ = teachers :: sectionTeacher.oneToMany :: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne
val fredAndHisStudents: (Teacher,Set[(Section,Set[(Enrollment,Student)])]) = run(teacherAndStudentsQ(fred.teacher)).get

```
"Nested Joins" are used to join multiple foreign keys to one parent
table with the `:::` operator.
```
val sectionWithTeacherAndStudents = (sections :: sectionTeacher.manyToOne) ::: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne
val sectionJoin: (Section, Teacher, Set[(Enrollment, Student)]) = run(sectionWithTeacherAndStudents(trigSection1.section)).get
```
In the above example, rows from both `Teacher` and `Enrollment` are joined to the same `Section` via the `sectionTeacher` and `enrollmentSection` relationships, respectively.  But `Student` is joined to `Enrollment` via the `enrollmentStudent` relationship.

### Adding Additional Where Clauses

Any query can be further restricted using a doobie fragment 
 ```
run(for {
   _ <- teachers.insert(Teacher(TeacherId(5), "John Smith"))
   _ <- teachers.insert(Teacher(TeacherId(6), "John Jones"))
   _ <- teachers.insert(Teacher(TeacherId(7), "Jim Jones"))
   johns <- teachers.scan.where(doobie.Fragment.const("name like 'John%'"))
} yield {
   assert(johns.map(_.teacher) == Set(TeacherId(5),TeacherId(6)))
})
```
There is an explicit shortcut for "in" Fragments. 
```
run(for {
   johns <- teachers.in(TeacherId(5), TeacherId(6))
} yield {
   assert(johns.map(_.teacher) == Set(TeacherId(5),TeacherId(6)))
})
```
Unfortunately this is of limited use right now because in queries do not work for
composite primary keys in Doobie.

### Using Raw SQL
As a final "escape hatch", a View object can be defined for any SQL query 
```
val sectionCountByCourse = View[CourseId, SectionCount](
    "select section_course_id, count(*) as count from Section group by section_course_id"
)
val sectionCount: Set[SectionCount] = run(sectionCountByCourse(trigId))
```
## ScarmConfig and Column Naming Conventions

There are some  options in the `ScarmConfig` object that influence how field names in case classes are mapped to column mames in database tables.

### Snake Case

By default, camel case field names (e.g. `aFieldName`) are mapped to snake case column names (`a_field_name`).  This can be turned off by setting `ScarmConfig.snakeCase` to `false`.  The field name is then used verbatim.

### Field Name Separators.

Column names for nested fields are constructed by joining the nested field names together with a field name separator.  For example, given this
```
case class InnerMost(x: Int)
case class Inner(inner: InnerMost)
case class Entity(
   ...
   amount: Inner
   ...
) 
```
the field `amount` is mapped to a column `amount_inner_x`.  This `_` character is used to separate the field names by default, but any character (or string) can be used by setting `ScarmConfig.fieldNameSeparator`.

### Prefixing Primary Keys

Since the first field is mapped to the primary key, it follows from the previous discussion that all columns in a composite primary key have the name of the primary key field (in the case class) as a prefix.  But this isn't the usual convention for composite primary keys in many-to-many relationship tables.  For example, consider a table like this:
```
CREATE TABLE order (
    customer_id INT NOT NULL,
    order_id INT NOT NULL,
    price INT NOT NULL,
    PRIMARY KEY (customer_id, order_id)
);

```
These model classes
```
case class OrderId(customerId: Int, orderId: Int)
case class Order(id: OrderId, price: Int)
```
map, by default, to the following table
```
CREATE TABLE order (
    id_customer_id INT NOT NULL,
    id_order_id INT NOT NULL,
    price INT NOT NULL,
    PRIMARY KEY (id_customer_id, id_order_id)
);

```
which isn't quite right.  But setting `ScarmConfig.prefixPrimaryKey` to `false`, drops the `id_` prefix from the primary key columns, which gives the correct primary key column names.

### Dropping Suffixes from AnyVal Columns

A common pattern in Scala is to wrap a primitive value in a case class to specify it's purpose, and then to  extend `AnyVal` so the run time environment treats the case class as a primitive value.
```
case class EntityId(id: Long) extends AnyVal
case class Entity(entityId: EntityId, account: Int)
```
`Entity` would, by default,  be mapped to a table like this
```
CREATE TABLE Entity(
    entity_id_id: BigInt PRIMARY KEY,
    account: Int NOT NULL
)
```
It might be preferable for the primary key to be called `entity_id` instead of `entity_id_id`.  One way to accomplish this is to drop the `Id` from the field name in the case class:
```
case class Entity(entity: EntityId, account: Int)
```
But given the common convention of naming fields after their types, this can get confusing.  Another alternative is to set
`ScarmConfig.suffixAnyVal` to `false`.  When this is set, the inner field name of any `AnyVal` field (primary key or not) is not considered as part of the column name.
