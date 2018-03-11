package com.vivi.scarm

import java.util.Calendar
import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.FunSuite

case class TeacherId(id: Int) extends AnyVal
case class CourseId(id: Int) extends AnyVal
case class StudentId(id: Int) extends AnyVal
case class SectionId(courseId: CourseId, semester: Int, number: Int)
case class EnrollmentId(studentId: StudentId, sectionId: SectionId)

case class Teacher(id: TeacherId, name: String) extends Entity[TeacherId]
case class Course(id: CourseId, subject: String, prerequisite: CourseId) extends Entity[CourseId]

case class Section(id: SectionId, instructor: TeacherId, 
  room: String, time: Calendar) extends Entity[SectionId]

case class Student(id: StudentId, name: String, level: Int)
    extends Entity[StudentId]

case class Enrollment(id: EnrollmentId, grade: Option[Char])
    extends Entity[EnrollmentId]


class DSLTest extends FunSuite {
  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections",
    Seq("courseId", "semester", "number"))
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments",
    Seq("studentId", "courseId", "semester", "number"))

  val sectionsBySemester = Index("sectionsBySemester", sections,
    Seq("semester")
  )

  val instructor = MandatoryForeignKey(sections,
    (s: Section) => s.instructor, teachers,
    Seq("instructor")
  )

  val prerequisite = OptionalForeignKey(courses,
    (c: Course) => c.prerequisite, courses,
    Seq("prerequisite"))

  val sectionCourse = MandatoryForeignKey(sections,
    (s: Section) => s.id.courseId, courses,
    Seq("courseId"))

  val enrollmentSection = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.sectionId, sections,
    Seq("courseId", "semester", "number"))

  val enrollmentStudent = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.studentId, students,
    Seq("studentId"))

  test("teachers sql") {
    assert(teachers.sql == "SELECT t1.* FROM teachers AS t1 WHERE t1.id=?")
  }

  test("courseWithTeacher sql") {
    val courseWithTeacher = sections :: instructor
    assert(courseWithTeacher.sql == "SELECT t1.*,t2.* FROM sections AS t1  LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.courseId=? AND t1.semester=? AND t1.number=?")
  }

  test("teacherWithSections sql") {
    val teacherWithSections = (teachers :: instructor.oneToMany)
    assert(teacherWithSections.sql == "SELECT t1.*,t2.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor WHERE t1.id=?")
  }

  test("teacherWithSectionsAndStudents") {
    val teacherWithSectionsAndStudents =
      (teachers :: instructor.reverse :: enrollmentSection.oneToMany :: enrollmentStudent)
    assert(teacherWithSectionsAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN enrollments AS t3 ON t2.courseId = t3.courseId AND t2.semester = t3.semester AND t2.number = t3.number  LEFT OUTER JOIN students AS t4 ON t3.studentId = t4.id WHERE t1.id=?")
  }

  test("teacherWithCourses sql") {
    val teacherWithCourses = teachers :: instructor.reverse :: sectionCourse
    assert(teacherWithCourses.sql == "SELECT t1.*,t2.*,t3.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN courses AS t3 ON t2.courseId = t3.id WHERE t1.id=?");
  }

  test("teacherWithCoursesAndStudents sql") {
    val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse) ::: enrollmentSection.reverse :: enrollmentStudent
    assert(teacherWithCoursesAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN courses AS t3 ON t2.courseId = t3.id  LEFT OUTER JOIN enrollments AS t4 ON t2.courseId = t4.courseId AND t2.semester = t4.semester AND t2.number = t4.number  LEFT OUTER JOIN students AS t5 ON t4.studentId = t5.id WHERE t1.id=?")
  }

  test("print out some sql") {

    val teacher = teachers
    val teacherStream: Stream[ConnectionIO,Teacher]  = teacher.query(TeacherId(1))

    val courseWithTeacher = sections :: instructor
    val courseWithTeacherStream: Stream[ConnectionIO, (Section,Teacher)] =
      courseWithTeacher.query(SectionId(CourseId(1), 1, 1))

    val teacherWithSections = (teachers :: instructor.oneToMany)
    val teacherWithSectionsStream: Stream[ConnectionIO, (Teacher,Set[Section])] =
      teacherWithSections.query(TeacherId(1))

    val teacherWithSectionsAndStudents =
      (teachers :: instructor.reverse :: enrollmentSection.oneToMany :: enrollmentStudent)
    val teacherWithSectionsAndStudentsStream
        :Stream[ConnectionIO, (Teacher,Set[(Section,Set[(Enrollment, Student)])])] =
      teacherWithSectionsAndStudents.query(TeacherId(1))


    val teacherWithCourses = teachers :: instructor.reverse :: sectionCourse
    val teacherWithCoursesStream: Stream[ConnectionIO, (Teacher,Set[(Section,Course)])] =
      teacherWithCourses.query(TeacherId(1))

    val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse) ::: enrollmentSection.reverse :: enrollmentStudent
    val teacherWithCoursesAndStudentsStream:
        Stream[ConnectionIO, (Teacher,Set[(Section,Course, Set[(Enrollment,Student)])])] =
      teacherWithCoursesAndStudents.query(TeacherId(1))
  }
}

