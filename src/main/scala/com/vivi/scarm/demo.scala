package com.vivi.scarm

import doobie._, doobie.implicits._
import cats.effect.IO
import doobie.util.transactor.Transactor
import java.util.UUID

object Demo {

  def main: Unit = {
    val xa = Transactor.fromDriverManager[IO](
      //        "org.h2.Driver", "jdbc:h2:mem:db" , "" , ""
      "org.postgresql.Driver", "jdbc:postgresql:cccassess" , "cccassess" , "cccassess"
    )
    def run[A](query: ConnectionIO[A]): A =
      query.transact(xa).unsafeRunSync

    val createPeople: ConnectionIO[Int] =
      sql"""create table if not exists people
          ( id char(36) not null primary key
          , name varchar(512) not null
          , email varchar(512) not null 
          )
     """.update.run

    run(createPeople)

    val createCourse: ConnectionIO[Int] =
      sql"""create table if not exists course
          ( id char(36) not null primary key
          , name varchar(512) not null
          , teacherId varchar(36) references people
          )
     """.update.run

    run(createCourse)

    case class NewPerson(name: String, email: String)

    implicit val UUIDMeta: Meta[UUID] =
      Meta[String].xmap(UUID.fromString, _.toString)

    case class Person(id: UUID, name: String, email: String)

    def addPerson(x: Person): ConnectionIO[Int] =
      sql"""insert into people (id, name, email)
        values ( ${x.id}
               , ${x.name}
               , ${x.email}
               )
     """.update.run

    case class NewCourse(name: String, teacherId: UUID)

    def addCourse(x: NewCourse): ConnectionIO[Int] =
      sql"""insert into course (id, name, teacherId)
        values ( ${UUID.randomUUID}
               , ${x.name}
               , ${x.teacherId}
               )
     """.update.run

    case class Course(id: UUID, name: String, teacherId: UUID)

    def getPeople: ConnectionIO[List[Person]] =
      (sql"""select id, name, email
         from people
      """.query[Person]
      ).list

    def getCourse: ConnectionIO[List[Course]] =
      (sql"""select *
         from course
      """.query[Course]
      ).list

    val p1 = Person(UUID.randomUUID(),"James Earl Douglas", "james@earldouglas.com")
    val p2 = Person(UUID.randomUUID(),"Johnny McDoe", "johnny@mcdoe")
    val c1 = NewCourse("Johnny I", p2.id)
    val c2 = NewCourse("Johnny II", p2.id)

    run(addPerson(p1))
    run(addPerson(p2))
    run(addCourse(c1))
    run(addCourse(c2))

    val l = run(getPeople)

    l.foreach {
          case Person(id, name, email) =>
            println()
            println(s"## ${name}")
            println()
            println(s"* ID: ${id.toString}")
            println(s"* Email: ${email}")
    }

    val l2 = run(getCourse)
    l2.foreach { case c => println(s"${c}") }

    case class CourseWithTeacher(course: Course, teacher: Person)
    def getCoursesWithTeacher: ConnectionIO[List[CourseWithTeacher]] = 
      (sql"""select co.*, p.*
         from course co 
         inner join people p on co.teacherId = p.id
      """.query[CourseWithTeacher]
      ).list

    run(getCoursesWithTeacher).foreach { case co => println(s"${co}") }

    case class TeacherWithCourses(teacher: Person, courses: Course)
    def getTeacherWithCourses: ConnectionIO[List[TeacherWithCourses]] = 
      (sql"""select p.*, co.*
         from people p
         inner join course co on co.teacherId = p.id
      """.query[TeacherWithCourses]
      ).list
    println(run(getTeacherWithCourses))

    def getPeopleT: ConnectionIO[List[(Person)]] =
      (sql"""select id, name, email
         from people
      """.query[(Person)]
      ).list

    println("getPeopleT")
    println(run(getPeopleT))

    def getTeacherWithCoursesT: ConnectionIO[List[(Person,Course)]] = 
      (sql"""select p.*, co.*
         from people p
         inner join course co on co.teacherId = p.id
      """.query[(Person,Course)]
      ).list
    println(run(getTeacherWithCoursesT))

    def getTeacherWithCoursesT2: ConnectionIO[List[(Person,Course,Person)]] =
      (sql"""select p.*, co.*, p2.*
         from people p
         inner join course co on co.teacherId = p.id
         inner join people p2 on co.teacherId = p2.id
      """.query[(Person,Course,Person)]
      ).list
    println(run(getTeacherWithCoursesT2))

//    implicit val exampleCourse = Course(UUID.randomUUID(), "foo", UUID.randomUUID())
/*
    implicit def optionalComposite[A](composite: Composite[A])(implicit example: A): Composite[Option[A]] =
      composite.imap(
        (a:A) => Some(a): Option[A]
      )((optionA: Option[A]) => optionA match {
        case None => example
        case Some(aa) => aa
      })
 */
//    implicit val optionalCourseComposite =  doobie.util.composite.Composite.ogeneric[Course]
//      optionalComposite[Course](Composite[Course])
/*
    implicit lazy val optionalCourseComposite: Composite[Option[Course]] =
      //      Composite[(Option[String], Option[String], Option[String])].xmap(
      Composite[(Option[String], Option[String], Option[String])].imap(
        (t: (Option[String], Option[String], Option[String])) =>
        t._1 match {
          case None => None
          case Some(t1) => Some(Course(UUID.fromString(t1), t._2.get, UUID.fromString(t._3.get)))
        })(
        (t: Option[Course]) => t match {
          case None => (None, None, None)
          case Some(co) => (Some(co.id.toString), Some(co.name), Some(co.teacherId.toString))
        }
      )*/

    //    def getTeacherWithCoursesOptT: ConnectionIO[List[(Person,Option[Course])]] =
    def getTeacherWithCoursesOptT: ConnectionIO[List[(Person, Option[Course])]] =
      (sql"""select p.*, co.*
         from people p
         left outer join course co on co.teacherId = p.id
      """.query[(Person, Option[Course])]
      ).list
    println(run(getTeacherWithCoursesOptT))
  }
}
