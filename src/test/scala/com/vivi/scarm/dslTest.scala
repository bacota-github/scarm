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
case class Course(id: CourseId, subject: String, prerequisite: Option[CourseId]) extends Entity[CourseId]

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
    Seq("student_id", "course_id", "semester", "section_number"))

  val sectionsBySemester = Index("sectionsBySemester", sections,
    (s: Section) => Some(s.id.semester), Seq("semester")
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
    Seq("student_id"))

  val courseWithTeacher = sections :: instructor
  val teacherWithSections = (teachers :: instructor.oneToMany)

  val teacherWithSectionsAndStudents =
      (teachers :: instructor.reverse :: enrollmentSection.oneToMany :: enrollmentStudent)

  val teacherWithCourses = teachers :: instructor.reverse :: sectionCourse

  val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse) ::: enrollmentSection.reverse :: enrollmentStudent

  val sectionsBySemesterWithInstructors =
    sectionsBySemester :: instructor

  test("sql for querying a table by primary key") {
    //println(teachers.sql)
    assert(teachers.sql == "SELECT t1.* FROM teachers AS t1 WHERE t1.id=? ORDER BY t1.id")
  }

  test("sql for a many to one join") {
    //println(courseWithTeacher.sql)
    assert(courseWithTeacher.sql == "SELECT t1.*,t2.* FROM sections AS t1 LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.course_id=? AND t1.semester=? AND t1.section_number=? ORDER BY t1.course_id,t1.semester,t1.section_number,t2.id")
  }

  test("sql for one to many join") {
    //println(teacherWithSections.sql)
    assert(teacherWithSections.sql == "SELECT t1.*,t2.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor WHERE t1.id=? ORDER BY t1.id,t2.course_id,t2.semester,t2.section_number")
  }

  test("sql for three table join") {
    //println(teacherWithCourses.sql)
    assert(teacherWithCourses.sql == "SELECT t1.*,t2.*,t3.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id WHERE t1.id=? ORDER BY t1.id,t2.course_id,t2.semester,t2.section_number,t3.id")
  }

  test("sql for four table join") {
    //println(teacherWithSectionsAndStudents.sql)
    assert(teacherWithSectionsAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN enrollments AS t3 ON t2.course_id = t3.course_id AND t2.semester = t3.semester AND t2.section_number = t3.section_number LEFT OUTER JOIN students AS t4 ON t3.student_id = t4.id WHERE t1.id=? ORDER BY t1.id,t2.course_id,t2.semester,t2.section_number,t3.student_id,t3.course_id,t3.semester,t3.section_number,t4.id")  }

  test("sql for join with two tables joined to root table") {
    //println(teacherWithCoursesAndStudents.sql)
    assert(teacherWithCoursesAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.*,t5.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id LEFT OUTER JOIN enrollments AS t4 ON t2.course_id = t4.course_id AND t2.semester = t4.semester AND t2.section_number = t4.section_number LEFT OUTER JOIN students AS t5 ON t4.student_id = t5.id WHERE t1.id=? ORDER BY t1.id,t2.course_id,t2.semester,t2.section_number,t3.id,t4.student_id,t4.course_id,t4.semester,t4.section_number,t5.id")
  }

  test("sql for query by index") {
    //println(sectionsBySemester.sql)
    assert(sectionsBySemester.sql == "SELECT t1.* FROM sections AS t1 WHERE t1.semester=? ORDER BY t1.course_id,t1.semester,t1.section_number")
  }

  test("sql for join on index") {
    //println(sectionsBySemesterWithInstructors.sql)
    assert(sectionsBySemesterWithInstructors.sql =="SELECT t1.*,t2.* FROM sections AS t1 LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.semester=? ORDER BY t1.course_id,t1.semester,t1.section_number,t2.id")
  }

}
