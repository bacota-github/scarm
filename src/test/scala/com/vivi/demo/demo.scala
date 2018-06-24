package com.vivi.scarm.test.demo

import cats.effect.IO
import doobie.ConnectionIO
import doobie.util.transactor.Transactor
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import com.vivi.scarm._
import java.time._

import doobie.implicits._
import shapeless._

/* Case classes to represent the data model. */

/*
 A Teacher is an entity with an Int primary key.  But rather than using
 a raw Int, we wrap it in a case class because, "type all the things".

 The first field in the case class is always the primary key (and the
 entire primary key).

 The name of the primary key column will be "teacher_id", constructed
 from the names of the fields called "teacher" and "id".
 */
case class TeacherId(id: Int) extends AnyVal
case class Teacher(teacher: TeacherId, name: String)

/* Key class for an index teacher name */
case class TeacherName(name: String)

/* Incorrect key classes for teacher indexes */
case class BadTeacherName(maim: String)
case class BadTeacherNameType(name: Int)


/*
 A Course is structured similarly to a Teacher, but there is an
 optional relationship from Course to itself, represented by the
 prerequisite field.  

 Field names are course_id, subject, and prerequisite_id.

 The prerequisite_id column would be nullable with nulls mapped to "None".
 */
case class CourseId(id: Int) extends AnyVal
case class Course(course: CourseId, subject: String, prerequisite: Option[CourseId])

/*
 A Section has a composite primary key with fields named
 section_course_id, section_semester, and section_number.

 There are (mandatory) foreign keys to Course and to Teacher, but the
 foreign key to Course is embedded in the Section primary key.

 Also note the java.time fields, which can be stored in timestamp
 columns.  By default, the corresponding database columns are expected
 to use snake case, so they should be called "meeting_time",
 "start_date", and "end_date", but this behavior can be changed in the
 ScarmConfig object.
 */
case class SectionId(course: CourseId, semester: Int, number: Int)
case class Section(section: SectionId,
  instructor: TeacherId,
  room: String,
  meetingTime: LocalTime,
  startDate: LocalDate,
  endDate: LocalDate
)

/* Key class for an index on a table of Section */
case class SectionSemester(semester: Int)
case class SectionIndex(room: String, section: SectionSemester, meetingTime: LocalTime)

/* Key class for a foreign key */
case class SectionInstructor(instructor: TeacherId)

/* Incorrect key classes for a foreign key on Section */
case class BadSectionInstructorName(xinstructor: TeacherId)
case class BadSectionSemester(section: SectionSemester)

/**
  Student has a structure similar to Teacher.
  */
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

case class Demo(config: ScarmConfig, xa: Transactor[IO])
    extends FunSuite with BeforeAndAfterAll {

  //ScarmConfig and Transactor are implicitly used in scarm methods
  implicit val scarmConfig = config
  implicit val scarmXa = xa

  def run[T](op: ConnectionIO[T]) =   op.transact(xa).unsafeRunSync()

  override def afterAll() {
    if (config.dialect == Hsqldb) {
      com.vivi.scarm.test.DSLSuite.hsqldbCleanup(xa)
    } else 
      Seq(Table[TeacherId,Teacher]("Teacher"),
        Autogen[CourseId,Course]("Course"),
        Table[SectionId,Section]("Section"),
        Table[StudentId,Student]("Student"),
        Table[EnrollmentId, Enrollment]("enrollment")
      ).foreach { t => run(t.drop) }
  }

  test("Demo all the things") {

    /*
     Define a table to hold teachers.
     
     Table objects proxy for tables in the relational database.  

     "teachers" is an object that proxies for a table called "Teacher"
     in the database.

     A Table's type parameters specify the type of the primary key and
     of the entity stored in the table.
     */

    val teachers = Table[TeacherId,Teacher]("Teacher")

    /**
      Create the table in the database.  

      Actually this should be done using a migration tool like flyway.
      But creating from the application is useful for unit/integration
      tests.
      */

    val createTeachersOp: ConnectionIO[Unit] = teachers.create

    /**
      Run the actual create command in the doobie manner.
      */
    createTeachersOp.transact(xa).unsafeRunSync()

    /*
     Since the primary key is always the first field of the case class,
     it should be possible to use shapeless to infer the primary key
     type, so we could just used

     val teachers = Table[Teacher]("Teacher")

     But, alas, I haven't gotten that to work.  

     On the bright side, declaring the wrong primary key type results in
     a compilation error "could not find implicit value for parameter primaryKey".
     */
    assertDoesNotCompile(
      "val teachers = Table[CourseId,Teacher](\"Teacher\")"
    )

    /**
      Now we can run some DML and selects on the table.
      */
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
    }  yield {
      assert(numInserted == 2)
      assert(shouldBeFred == Some(fred))
      assert(shouldBeNone == None)
      assert(numUpdated == 2)
      assert(shouldBeFreddie == Some(freddie))
    }

    op.transact(xa).unsafeRunSync()

    /* We can also delete rows. At the moment, deleting by N id's results
     * in N separate queries being issued to the database. */
    val deleteOp = for {
      nDeleted <- teachers.delete(fred.teacher, robert.teacher)
      fredFound <- teachers(fred.teacher)
    } yield {
      assert(nDeleted == 2)
      assert(fredFound == None)
    }

    deleteOp.transact(xa).unsafeRunSync()

    /*
     Autogen creates a Table object with an autogenerated primary key
     using an auto_increment field in mysql or a sequence in hibernate.
     */
    val courses = Autogen[CourseId,Course]("Course")

    /* run(_) is an abbreviation for _.transact(xa).unsafeRunSync() */
    run(courses.create)

    /* Algebra has no prerequisite */
    val algebraPrototype = Course(CourseId(0), "Algebra I", None)

    /*
     When we insert an entity with an autogenerated key, we usually
     need to return the actual key. Note that algebra's original key is ignored.     */
    val algebraId = run(courses.insertReturningKey(algebraPrototype))

    val algebra = algebraPrototype.copy(course=algebraId)

    /* The returned algebraId is used as the prerequisite for Trigonometry */
    val trigPrototype = Course(CourseId(0), "Trigonometry", Some(algebraId))
    val trigId = run(courses.insertReturningKey(trigPrototype))
    val trig = trigPrototype.copy(course=trigId)

    /* Selecting by a returned Id returns the inserted object, but with the new Id */
    assert(run(courses(algebraId)) == Some(algebra.copy(course=algebraId)))
    assert(run(courses(trigId)) == Some(trig.copy(course=trigId)))

    val sections = Table[SectionId,Section]("Section")
    val students = Table[StudentId,Student]("Student")
    val enrollments = Table[EnrollmentId, Enrollment]("enrollment")

    Seq(sections, students, enrollments).foreach {
      t => run(t.create)
    }

    /* All rows are returned by a scan. */
    assert(Set(algebra, trig) == run(courses.scan(Unit)))


    /* To query a table on columns other than the primary key, create an index.
     The index below has three type parameters
     1. The indexed key type  (TeacherByName) defines the column(s) on which to query
     2. The primary key type (TeacherId) is the primary key of the table.
     The primary key type is redundant, but Ihaven't been able to get rid of it.
     3. The entity type (Teacher) is the type of entity stored in the table.
     The index has one parameter -- a Table[TeacherId,Teacher]
     */
    val teachersByName = Index(teachers, classOf[TeacherName])

    /* The table can now be queried by name */
    val tom1 = Teacher(TeacherId(3), "Tom")
    val tom2 = Teacher(TeacherId(4), "Tom")
    run(teachers.insert(tom1,tom2))
    assert(run(teachersByName(TeacherName("Tom"))) == Set(tom1,tom2))

    /* We can use a UniqueIndex to return an Option instead of a Set. */
    val uniqueTeacherByName = UniqueIndex(teachers, classOf[TeacherName])
    run(teachers.delete(tom2.teacher))  //or an exception will be thrown by the query
    assert(run(uniqueTeacherByName(TeacherName("Tom"))) == Some(tom1))

    /* Through the magic of shapeless, if the fields in the key class
     * can't be found in the entity type, there will be compiler error
     * "could not find implicit value for parameter isProjection"  
     */
    assertDoesNotCompile(
      "val teacherByBadName = Index(teachers,classOf[BadTeacherName])"
    )
    assertDoesNotCompile(
      "val teacherByBadlyTypedName = Index(teachers, classOf[BadTeacherNameType])"
    )

    /* An index can be created in the database, though this is not necessary. */
    run(teachersByName.create)

    /* Multicolumn indexes work, too, but each field in the key class
     * must have a corresponding field of the same type in the entity
     * class, and the column names generated for those fields must
     * match. 
     */
    val sectionByRoom = Index(sections, classOf[SectionIndex])
    val roomKey = SectionIndex("12A", SectionSemester(1), LocalTime.of(14,0))
    //Note that we didn't insert any sections, so we get back an empty set
    assert(run(sectionByRoom(roomKey)) == Set())

    /* Now define one of the relationships in the data model by a foreign key */
    val sectionTeacher = MandatoryForeignKey(sections, teachers, classOf[SectionInstructor])

    /* As with indexes, each field in the key class ("SectionInstructor"
     * in this case) must have a corresponding field of the same type
     * in the entity class, and the column names generated for those
     * fields must match.  
     * 
     *  The following fails to compile with the error "could not find
     *  implicit value for parameter foreignKeyIsSubsetOfChildEntity
     *  .." because the name of the field in BadSectionInstructorName
     *  is mispelled.
     */
    assertDoesNotCompile(
      "val badSectionTeacher1 = MandatoryForeignKey(sections, teachers, classOf[BadSectionInstructorName])"
    )

    /* Furthermore, the types (underlying HList) of the key class must
     * align exactly with those of the referenced class's primary
     * key.   
     * 
     *  The following fails to compile with the error "could not find
     *  implicit value for parameter
     *  foreignKeyStructureMatchesPrimaryKey ..."
     */
    assertDoesNotCompile(
      "val badSectionTeacher2 = MandatoryForeignKey(sections, teachers, classOf[BadSectionSemester])"
    )

    /* Pause to populate some tables */
    run(teachers.insert(fred,robert))
    val trigSection1 =
      Section(SectionId(trigId, 1, 1), fred.teacher, "Olin Hall 101", 
      LocalTime.of(1,0), LocalDate.of(2018,9,3), LocalDate.of(2018,12,3))
    val trigSection2 =
      Section(SectionId(trigId, 1, 2), fred.teacher, "Olin Hall 101", 
      LocalTime.of(2,30), LocalDate.of(2018,9,3), LocalDate.of(2018,12,3))
    val trigSection3 =
      Section(SectionId(trigId, 1, 3), robert.teacher, "Olin Hall 102", 
      LocalTime.of(2,30), LocalDate.of(2018,9,3), LocalDate.of(2018,12,3))

    run(sections.insert(trigSection1,trigSection2,trigSection3))

    /* Every foreign key comes with an index that can be used to query the
     * child table based on the primary key of the parent table. */
    assert(
      run(sectionTeacher.index(fred.teacher)) ==
        Set(trigSection1, trigSection2)
    )

    /* The most important use of foreign keys is to construct join
     *  queries.  The "oneToMany" property of the sectionTeacher
     *  foreign key is used to construct a join from the one side
     *  (teachers) to the many side (sections).
     */

    val teacherWithSectionsQ = teachers :: sectionTeacher.oneToMany

    /* Given a teacher primary key, the above query returns a Teacher (if
     * found) with all the sections taught by the teacher. */
    val fredAndHisSections: Option[(Teacher, Set[Section])] =
      run(teacherWithSectionsQ(fred.teacher))
    assert(fredAndHisSections.get == (fred, Set(trigSection1, trigSection2)))

    /* Indexes can also be joined with. Note that the query returns a Set
     * instead of an Option.  Joining with a UniqueIndex would return
     * an Option.  */
    val teachersByNameWithSectionsQ = teachersByName :: sectionTeacher.oneToMany
    val fredSections: Set[(Teacher, Set[Section])] = run(teachersByNameWithSectionsQ(TeacherName("Fred")))
    assert(fredSections == Set((fred, Set(trigSection1,trigSection2))))

    /* Join queries are outer joins. */
    val mary = Teacher(TeacherId(4), "Mary")
    run(for {
      _ <- teachers.insert(mary)
      maryAndHerClasses <- teacherWithSectionsQ(mary.teacher)
    } yield {
      assert(maryAndHerClasses == Some((mary, Set())))
    })

    /* Joins also work in the manyToOne direction. The result set consists
     *  of pairs of entities, instead of an entity and a Set. */
    val sectionWithTeachersQ = sections :: sectionTeacher.manyToOne
    val sectionAndTeacher: Option[(Section,Teacher)] = run(sectionWithTeachersQ(trigSection1.section))
    assert(sectionAndTeacher.get == (trigSection1, fred))

    /* An optional foreign key is similar to a Mandatory Key, but some of
     * the fields can be Option.  Also note that the following is a
     * self-referential relationship. */
    val prerequisite = OptionalForeignKey(courses, courses, classOf[CoursePrerequisite])

    /* One other difference of optional foreign keys is that when used in
     * manyToOne joins, the result set is a pair of an entity and an
     * Optional entity */
    val courseAndPrerequisiteQ = courses :: prerequisite.manyToOne
    val courseAndPrerequisite: Option[(Course,Option[Course])] =
      run(courseAndPrerequisiteQ(trigId))
    assert(courseAndPrerequisite.get == (trig, Some(algebra)))

    /* More foreign keys */
     val sectionCourse = MandatoryForeignKey(sections, courses, classOf[SectionCourse])
     val enrollmentSection = MandatoryForeignKey(enrollments, sections, classOf[EnrollmentSection])
    val enrollmentStudent = MandatoryForeignKey(enrollments, students, classOf[EnrollmentStudent])

    /* Joins can be chained in any logical way */

    val teacherAndStudentsQ = teachers :: sectionTeacher.oneToMany ::
        enrollmentSection.oneToMany :: enrollmentStudent.manyToOne

    val fredAndHisStudents: (Teacher,Set[(Section,Set[(Enrollment,Student)])]) =
      run(teacherAndStudentsQ(fred.teacher)).get

    /* "Nested Joins" are used to join multiple foreign keys to one parent
     * table with the ::: operator. */

    val sectionWithTeacherAndStudents =
      (sections :: sectionTeacher.manyToOne) :::
      enrollmentSection.oneToMany :: enrollmentStudent.manyToOne

    val trigSectionJoin: (Section, Teacher, Set[(Enrollment, Student)]) =
      run(sectionWithTeacherAndStudents(trigSection1.section)).get

    /* Any query can be further restricted using a doobie fragment */
    run(for {
      _ <- teachers.insert(Teacher(TeacherId(5), "John Smith"))
      _ <- teachers.insert(Teacher(TeacherId(6), "John Jones"))
      _ <- teachers.insert(Teacher(TeacherId(7), "Jim Jones"))
      johns <- teachers.scan.where(doobie.Fragment.const("name like 'John%'"))
    } yield {
      assert(johns.map(_.teacher) == Set(TeacherId(5),TeacherId(6)))
    })

    /* There is an explicit shortcut for "in" Fragments. Unfortunately
     * this is of limited use because in queries do not work for
     * composite primary keys.  */
    run(for {
      johns <- teachers.in(TeacherId(5), TeacherId(6))
    } yield {
      assert(johns.map(_.teacher) == Set(TeacherId(5),TeacherId(6)))
    })

    /* As a final "escape hatch", a View object can be defined for any query */
    val sectionCountByCourse = View[CourseId, SectionCount](
      "select section_course_id, count(*) as count from Section group by section_course_id"
    )
    assert(run(sectionCountByCourse(trigId)) == Set(SectionCount(trigId, 3)))
  }
}


class DemoCleanup(implicit config: ScarmConfig, xa: Transactor[IO]) {
  Seq(Table[TeacherId,Teacher]("Teacher"),
    Autogen[CourseId,Course]("Course"),
    Table[SectionId,Section]("Section"),
    Table[StudentId,Student]("Student"),
    Table[EnrollmentId, Enrollment]("enrollment")
  ).foreach { t => run(t.drop) }
}

