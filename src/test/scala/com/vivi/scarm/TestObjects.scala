package com.vivi.scarm.test

import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.FunSuite
import com.vivi.scarm._


object TestObjects {

  case class TeacherId(id: Int) extends AnyVal
  case class CourseId(id: Int) extends AnyVal
  case class StudentId(id: Int) extends AnyVal
  case class SectionId(course: CourseId, semester: Int, number: Int)
  case class EnrollmentId(student: StudentId, section: SectionId)
  case class AssignmentId(id: Int) extends AnyVal

  case class Teacher(id: TeacherId, name: String) extends Entity[TeacherId]

  case class Course(id: CourseId, subject: String,
    prerequisite: Option[CourseId]) extends Entity[CourseId]

  case class Section(id: SectionId, instructor: TeacherId,
    room: String, meetingTime: java.time.LocalTime,
    startDate: java.time.LocalDate, endDate: java.time.LocalDate)
      extends Entity[SectionId]

  case class Student(id: StudentId, name: String, level: Int)
      extends Entity[StudentId]

  case class Enrollment(id: EnrollmentId, grade: Option[String])
      extends Entity[EnrollmentId]

  case class Assignment(id: AssignmentId, name: String,
    dueDate: java.time.LocalDate, section: SectionId
  ) extends Entity[AssignmentId]

  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections")
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments")
  val assignments = Table[AssignmentId, Assignment]("assignments")

  val allTables = Seq(teachers,courses, sections, students,enrollments,assignments)

  val sectionsBySemester = Index("sectionsBySemester", sections,
    (s: Section) => Some(s.id.semester), Seq("semester")
  )

  val instructor = MandatoryForeignKey(sections,
    (s: Section) => s.instructor, teachers)

  val prerequisite = OptionalForeignKey(courses,
    (c: Course) => c.prerequisite, courses)

  val sectionCourse = MandatoryForeignKey(sections,
    (s: Section) => s.id.course, courses)

  val enrollmentSection = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.section, sections)

  val enrollmentStudent = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.student, students)

  val courseWithTeacher = sections :: instructor
  val teacherWithSections = (teachers :: instructor.oneToMany)

  val teacherWithSectionsAndStudents =
      (teachers :: instructor.reverse :: enrollmentSection.oneToMany :: enrollmentStudent)

  val teacherWithCourses = teachers :: instructor.reverse :: sectionCourse

  val teacherWithCoursesAndStudents =
      teachers :: (instructor.oneToMany :: sectionCourse) ::: enrollmentSection.reverse :: enrollmentStudent

  val sectionsBySemesterWithInstructors =
    sectionsBySemester :: instructor
}
