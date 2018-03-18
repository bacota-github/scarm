package com.vivi.scarm.test

import org.scalatest.FunSuite
import doobie._
import cats.effect.IO
import com.vivi.scarm._

import com.vivi.scarm.test.TestObjects._

class DSLIntegrationTest extends FunSuite {

  implicit val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver", "jdbc:postgresql:scarm" , "scarm" , "scarm"
  )

  test("After inserting an object, we can select the object by id") {
    val id = TeacherId(100)
    val newTeacher = Teacher(id, "Fred")
    teachers.insert(newTeacher)
    assert(teachers.query(TeacherId(100)) == newTeacher)
    teachers.delete(id)
    assert(teachers.primaryKey.query(TeacherId(100)) == None)
  }


  test("foo") {
    val teacherq = teachers.query(TeacherId(1))
    println("Teachers: " + teacherq)

    val sectionq = sections.query(SectionId(CourseId(1),1,1))
    println("Sections: " + sectionq)

    println("Course With Teachers: " +
      courseWithTeacher.query(SectionId(CourseId(1), 1, 1)))

    println("teacher with Sections: " +
      teacherWithSections.query(TeacherId(2)))

    println("teacher with sections and students: " +
      teacherWithSectionsAndStudents.query(TeacherId(1)))

    println("sql for teacher with courses: " + teacherWithCourses.sql)

    println("teacher with courses: " + teacherWithCourses.query(TeacherId(1)))

    println("teacher with courses and students: " +
      teacherWithCoursesAndStudents.query(TeacherId(1)))

    //need tests with outer joins that have no related entities
  }
}
