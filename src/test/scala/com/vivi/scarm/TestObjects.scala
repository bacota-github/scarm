package com.vivi.scarm.test

import java.util.Calendar
import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.FunSuite
import com.vivi.scarm._
import com.vivi.scarm.FieldNames._
import com.vivi.scarm.fl.FieldList
import com.vivi.scarm.fl.FieldList._


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

  val teacherWitness = Teacher(TeacherId(0), "")
  val courseWitness = Course(CourseId(0), "",None)
  val sectionId = SectionId(CourseId(0),0,0)
  val sectionWitness = Section(sectionId,TeacherId(0),"",null)
  val studentWitness = Student(StudentId(0),"",0)
  val enrollmentWitness = Enrollment(EnrollmentId(StudentId(0),sectionId),None)

  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections")
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments")

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
}
