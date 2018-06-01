package com.vivi.scarm.test

import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.FunSuite
import com.vivi.scarm._
import java.time._

import shapeless._

case class TeacherId(id: Int) extends AnyVal
case class Teacher(id: TeacherId, name: String)

case class CourseId(id: Int) extends AnyVal
case class Course(id: CourseId, subject: String, prerequisite: Option[CourseId])
case class CoursePrerequisite(prerequisite: Option[CourseId])

case class SectionId(course: CourseId, semester: Int, number: Int)
case class Section(id: SectionId, instructor: TeacherId,
  room: String, meetingTime: LocalTime,
  startDate: LocalDate, endDate: java.time.LocalDate)
case class SectionTeacher(instructor: TeacherId)
case class CourseIdInSectionId(course: CourseId)
case class SectionCourse(id: CourseIdInSectionId)

case class StudentId(id: Int) extends AnyVal
case class Student(id: StudentId, name: String, level: Int)

case class EnrollmentId(student: StudentId, section: SectionId)
case class Enrollment(id: EnrollmentId, grade: Option[String])

case class WrappedEnrollmentStudent(student: StudentId)
case class EnrollmentStudent(id: WrappedEnrollmentStudent)

case class WrappedEnrollmentSection(section: SectionId)
case class EnrollmentSection(id: WrappedEnrollmentSection)

case class AssignmentId(id: Int) extends AnyVal
case class Assignment(id: AssignmentId, name: String,
  dueDate: java.time.LocalDate, section: SectionId
)


case class TestObjects(dialect: SqlDialect) {

  implicit val theDialect = dialect

  val teachers = Autogen[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections")
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments")
  val assignments = Table[AssignmentId, Assignment]("assignments")

  val allTables = Seq(teachers,courses, sections, students,enrollments,assignments)

  val sectionsBySemester =
    Index[String,SectionId,Section]("sectionsBySemester", sections, Seq("semester"))

  implicit val flattenedSectionTeacher = Flattened[SectionTeacher, Int :: HNil]
  val instructor = ForeignKey(sections, teachers, classOf[SectionTeacher])

  implicit val flattenedPrerequisite = Flattened[CoursePrerequisite, Int::HNil]
  val prerequisite = ForeignKey(courses, courses, classOf[CoursePrerequisite])

  implicit val flattenedSectionId = Flattened[SectionId, Int::Int::Int::HNil]
  implicit val flattenedSectionCourse = Flattened[SectionCourse, SectionId::HNil]
  val sectionCourse = ForeignKey(sections, courses, classOf[SectionCourse])

  implicit val flattenedEnrollmentSection = Flattened[EnrollmentSection, Int::Int::Int::HNil]
  val enrollmentSection = ForeignKey(enrollments, sections, classOf[EnrollmentSection])

  implicit val flattenedEnrollmentStudent = Flattened[EnrollmentStudent, Int::HNil]
  val enrollmentStudent = ForeignKey(enrollments, students, classOf[EnrollmentStudent])

  val courseWithTeacher = sections :: instructor.manyToOne
  val teacherWithSections = (teachers :: instructor.oneToMany)

  val teacherWithSectionsAndStudents =
      (teachers :: instructor.oneToMany :: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne)

  val teacherWithCourses = teachers :: instructor.oneToMany :: sectionCourse.manyToOne

  val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse.manyToOne) ::: enrollmentSection.oneToMany :: enrollmentStudent.manyToOne

  val sectionsBySemesterWithInstructors =
    sectionsBySemester :: instructor.manyToOne
}
