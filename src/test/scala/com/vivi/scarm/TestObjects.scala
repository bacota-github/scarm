package com.vivi.scarm.test

import java.util.Calendar
import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.FunSuite
import com.vivi.scarm._


object TestObjects {

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

  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections")
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments")

  val sectionsBySemester = Index("sectionsBySemester", sections,
    (s: Section) => Some(s.id.semester), Seq("semester")
  )

  val instructor = MandatoryForeignKey(sections,
    (s: Section) => s.instructor, teachers)

  val prerequisite = OptionalForeignKey(courses,
    (c: Course) => c.prerequisite, courses)

  val sectionCourse = MandatoryForeignKey(sections,
    (s: Section) => s.id.courseId, courses)

  val enrollmentSection = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.sectionId, sections)

  val enrollmentStudent = MandatoryForeignKey(enrollments,
    (e: Enrollment) => e.id.studentId, students)

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
