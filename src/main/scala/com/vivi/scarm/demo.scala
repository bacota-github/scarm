package com.vivi.scarm

import doobie.imports._
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
          , email varchar(512) not null unique
          )
     """.update.run

    run(createPeople)

    val createCourse: ConnectionIO[Int] =
      sql"""create table if not exists course
          ( id char(36) not null primary key
          , name varchar(512) not null
          , teacherId varchar(36)
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
  }
}
