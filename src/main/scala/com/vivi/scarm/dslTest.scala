package com.vivi.scarm

import java.util.Calendar
import doobie.ConnectionIO
import fs2.Stream

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


class DSLTest {
  val teachers = Table[TeacherId,Teacher]("teachers")
  val courses = Table[CourseId,Course]("courses")
  val sections = Table[SectionId,Section]("sections")
  val students = Table[StudentId,Student]("students")
  val enrollments = Table[EnrollmentId, Enrollment]("enrollments")

  val sectionsBySemester = Index("sectionsBySemester", sections,
    (s: Section) =>  s.id.semester
  )

  val instructor = ForeignKey(sections,(s: Section) => s.instructor, teachers)
  val prerequisite = OptionalForeignKey(courses, (c: Course) => c.prerequisite, courses)
  val sectionCourse = ForeignKey(sections, (s: Section) => s.id.courseId, courses)
  val enrollmentCourse = ForeignKey(enrollments, (e: Enrollment) => e.id.sectionId, sections)
  val enrollmentStudent = ForeignKey(enrollments, (e: Enrollment) => e.id.studentId, students)

  val teacher: Stream[ConnectionIO,Teacher]  = teachers.query(TeacherId(1))

  val courseWithTeacher: Stream[ConnectionIO, (Section,Teacher)] =
    (sections :: instructor.manyToOne).query(SectionId(CourseId(1), 1, 1))

  val teacherWithSections: Stream[ConnectionIO, (Teacher,Set[Section])] =
    (teachers :: instructor.oneToMany).query(TeacherId(1))

  val teacherWithSectionsAndStudents
      :Stream[ConnectionIO, (Teacher,Set[(Section,Set[(Enrollment, Student)])])] =
    (teachers :: instructor.oneToMany :: enrollmentCourse.oneToMany ::
      enrollmentStudent.manyToOne).query(TeacherId(1))

  val teacherWithCourses: Stream[ConnectionIO, (Teacher,Set[(Section,Course)])] =
    (teachers :: instructor.oneToMany :: sectionCourse.manyToOne).query(TeacherId(1))

  val teacherWithCoursesAndStudents:
  Stream[ConnectionIO, (Teacher,Set[((Section,Course), Set[(Enrollment,Student)])])] =
    (teachers :: (instructor.oneToMany +: sectionCourse.manyToOne) :: enrollmentCourse.oneToMany :: enrollmentStudent.manyToOne
    ).query(TeacherId(1))
}

