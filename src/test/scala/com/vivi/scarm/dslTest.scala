package com.vivi.scarm.test

import org.scalatest.FunSuite

import com.vivi.scarm._

import TestObjects._

case class IntId(id: Int)
case class StringId(string: Int)

case class IntEntity(id: Int, name: String, intval: Option[String])
    extends Entity[Int]

case class StringEntity(id: StringId, name: String, strval: Option[String], intval: Option[Int])
    extends Entity[StringId]



class DSLTest extends FunSuite {


  test("create sql") {

    val intTable = Table[Int,IntEntity]("ints", Seq("id"))
    val stringTable = Table[StringId,StringEntity]("stringss")

    println(Table.createSql(teachers))
    println(Table.createSql(courses))
    println(Table.createSql(intTable))
    println(Table.createSql(stringTable))
  }

  test("sql for querying a table by primary key") {
    println(teachers.sql)
    //assert(teachers.sql == "SELECT t1.* FROM teachers AS t1 WHERE t1.id=?")
  }

  test("sql for a many to one join") {
    println(courseWithTeacher.sql)
    //assert(courseWithTeacher.sql == "SELECT t1.*,t2.* FROM sections AS t1 LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.course_id=? AND t1.semester=? AND t1.section_number=?")
  }

  test("sql for one to many join") {
    println(teacherWithSections.sql)
    //assert(teacherWithSections.sql == "SELECT t1.*,t2.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor WHERE t1.id=?")
  }

  test("sql for three table join") {
    println(teacherWithCourses.sql)
    //assert(teacherWithCourses.sql == "SELECT t1.*,t2.*,t3.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id WHERE t1.id=?")
  }

  test("sql for four table join") {
    println(teacherWithSectionsAndStudents.sql)
    //assert(teacherWithSectionsAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN enrollments AS t3 ON t2.course_id = t3.course_id AND t2.semester = t3.semester AND t2.section_number = t3.section_number LEFT OUTER JOIN students AS t4 ON t3.student_id = t4.id WHERE t1.id=?")
  }

  test("sql for join with two tables joined to root table") {
    println(teacherWithCoursesAndStudents.sql)
    //assert(teacherWithCoursesAndStudents.sql == "SELECT t1.*,t2.*,t3.*,t4.*,t5.* FROM teachers AS t1 LEFT OUTER JOIN sections AS t2 ON t1.id = t2.instructor LEFT OUTER JOIN courses AS t3 ON t2.course_id = t3.id LEFT OUTER JOIN enrollments AS t4 ON t2.course_id = t4.course_id AND t2.semester = t4.semester AND t2.section_number = t4.section_number LEFT OUTER JOIN students AS t5 ON t4.student_id = t5.id WHERE t1.id=?")
  }

  test("sql for query by index") {
    println(sectionsBySemester.sql)
    //assert(sectionsBySemester.sql == "SELECT t1.* FROM sections AS t1 WHERE t1.semester=?")
  }

  test("sql for join on index") {
    println(sectionsBySemesterWithInstructors.sql)
    //assert(sectionsBySemesterWithInstructors.sql =="SELECT t1.*,t2.* FROM sections AS t1 LEFT OUTER JOIN teachers AS t2 ON t1.instructor = t2.id WHERE t1.semester=?")
  }
}
