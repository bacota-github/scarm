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
  room: String, time: java.sql.Time) extends Entity[SectionId]

case class Student(id: StudentId, name: String, level: Int)
    extends Entity[StudentId]

case class Enrollment(id: EnrollmentId, grade: Option[String])
    extends Entity[EnrollmentId]


class DSLTest extends FunSuite {
  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections",
    Seq("course_id", "semester", "section_number"))
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments",
    Seq("studentId", "course_id", "semester", "section_number"))

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
    Seq("course_id"))

  val enrollmentSection = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.sectionId, sections,
    Seq("course_id", "semester", "section_number"))

  val enrollmentStudent = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.studentId, students,
    Seq("studentId"))

  val courseWithTeacher = sections :: instructor
  val teacherWithSections = (teachers :: instructor.oneToMany)

  val teacherWithSectionsAndStudents =
      (teachers :: instructor.reverse :: enrollmentSection.oneToMany :: enrollmentStudent)

  val teacherWithCourses = teachers :: instructor.reverse :: sectionCourse

  val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse) ::: enrollmentSection.reverse :: enrollmentStudent


  test("teachers sql") {
    assert(teachers.sql == "SELECT t1.* FROM teachers AS t1 WHERE t1.id=?")
  }

  test("courseWithTeacher sql") {
    assert(courseWithTeacher.sql == "SELECT t1.*,t2.* FROM sections AS t1  LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.course_id=? AND t1.semester=? AND t1.section_number=?")
  }

  test("teacherWithSections sql") {
    assert(teacherWithSections.sql == "SELECT t1.*,t2.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor WHERE t1.id=?")
  }

  test("teacherWithSectionsAndStudents") {
    assert(teacherWithSectionsAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN enrollments AS t3 ON t2.course_id = t3.course_id AND t2.semester = t3.semester AND t2.section_number = t3.section_number  LEFT OUTER JOIN students AS t4 ON t3.studentId = t4.id WHERE t1.id=?")
  }

  test("teacherWithCourses sql") {
    assert(teacherWithCourses.sql == "SELECT t1.*,t2.*,t3.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id WHERE t1.id=?");
  }

  test("teacherWithCoursesAndStudents sql") {
    assert(teacherWithCoursesAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1  LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor  LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id  LEFT OUTER JOIN enrollments AS t4 ON t2.course_id = t4.course_id AND t2.semester = t4.semester AND t2.section_number = t4.section_number  LEFT OUTER JOIN students AS t5 ON t4.studentId = t5.id WHERE t1.id=?")
  }

}
