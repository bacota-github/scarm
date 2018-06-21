package com.vivi.scarm.demo

import cats.effect.IO
import doobie.ConnectionIO
import doobie.util.transactor.Transactor
import fs2.Stream
import org.scalatest.FunSuite
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

/*
case class CoursePrerequisite(prerequisite: Option[CourseId])

case class SectionTeacher(instructor: TeacherId)
case class CourseIdInSectionId(course: CourseId)
case class SectionCourse(id: CourseIdInSectionId)


case class WrappedEnrollmentStudent(student: StudentId)
case class EnrollmentStudent(id: WrappedEnrollmentStudent)

case class WrappedEnrollmentSection(section: SectionId)
case class EnrollmentSection(id: WrappedEnrollmentSection)

case class AssignmentId(id: Int) extends AnyVal
case class Assignment(id: AssignmentId, name: String,
  dueDate: java.time.LocalDate, section: SectionId
 */


//ScarmConfig and Transactor are implicitly used in scarm methods
class Demo(implicit config:ScarmConfig, xa: Transactor[IO]) {
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
    
    The create command returns a ConnectionIO[Int] where the Int value
    is 1 on success.
    */

  val createTeachersOp: ConnectionIO[Unit] = teachers.create

  /**
    Run the actual create command in the doobie manner.
    */
  createTeachersOp.transact(xa).unsafeRunSync()

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

  /*
   Since the primary key is always the first field of the case class,
   it should be possible to use shapeless to infer the primary key
   type, so we could just used

   val teachers = Table[Teacher]("Teacher")

   But, alas, I haven't gotten that to work.  

   On the bright side, declaring the wrong primary key type results in
   a compilation error "could not find implicit value for parameter primaryKey".

   So, for example, the following would fail to compile:

   val teachers = Table[CourseId,Teacher]("Teacher")
   */

  /*
   Autogen creates a Table object with an autogenerated primary key
   using an auto_increment field in mysql or a sequence in hibernate.
   */
  val courses = Autogen[CourseId,Course]("Course")

  val sections = Table[SectionId,Section]("Section")
  val students = Table[StudentId,Student]("Student")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollment")


  /* Tables can be created in the database, but the main use case for
   * this is unit/integration testing.  I would maintain database
   * structures through a database migration utility like flyway.
   */
  val tables = Seq(courses, sections, students, enrollments)
  tables.foreach {
    t => run(t.create)
  }

  /*  val sectionsBySemester =
   Index[String,SectionId,Section]("sectionsBySemester", sections, Seq("semester"))

   val instructor = OptionalForeignKey(sections, teachers, classOf[SectionTeacher])
   val prerequisite = OptionalForeignKey(courses, courses, classOf[CoursePrerequisite])
   val sectionCourse = OptionalForeignKey(sections, courses, classOf[SectionCourse])
   val enrollmentSection = OptionalForeignKey(enrollments, sections, classOf[EnrollmentSection])
   val enrollmentStudent = OptionalForeignKey(enrollments, students, classOf[EnrollmentStudent])

   val courseWithTeacher = sections :: instructor.manyToOne
   val teacherWithSections = (teachers :: instructor.oneToMany)

   val teacherWithSectionsAndStudents =
   (teachers :: instructor.oneToMany :: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne)

   val teacherWithCourses = teachers :: instructor.oneToMany :: sectionCourse.manyToOne

   val teacherWithCoursesAndStudents =
   teachers :: (instructor.oneToMany :: sectionCourse.manyToOne) ::: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne

   val sectionsBySemesterWithInstructors =
   sectionsBySemester :: instructor.manyToOne
   */
}

class DemoCleanup(implicit config: ScarmConfig, xa: Transactor[IO]) {
  Seq(Table[TeacherId,Teacher]("Teacher"),
    Autogen[CourseId,Course]("Course"),
    Table[SectionId,Section]("Section"),
    Table[StudentId,Student]("Student"),
    Table[EnrollmentId, Enrollment]("enrollment")
  ).foreach { t => run(t.drop) }
}
